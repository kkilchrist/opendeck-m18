//! Live LED control for other local processes (fork addition).
//!
//! OpenDeck gives plugins no way to message each other, and only this plugin
//! can reach the M18's LEDs, so it listens on a Unix socket that only the
//! current user can open. One JSON object per line; one reply line each.
//!
//! ```text
//! {"colors": ["#rrggbb", ... 24]}           replace the whole palette
//! {"set": {"0": "#ff0000", "23": "#000"}}    change individual LEDs
//! {"restore": true}                          back to the Set LED Colors key's palette
//! {"get": true}                              reply with the live palette, brightness, standby
//! {"brightness": 30}                         hardware LED brightness, 0-100 (kept across reconnects)
//! {"standby": true}                          screen + LEDs dark; false (or any key press) wakes
//! ```
//!
//! Every request may carry `"device": "<id>"`; without it, every connected M18 is
//! targeted. Live palettes are held in memory only. The saved palette stays
//! whatever the Set LED Colors key last chose, so a restart comes back to it.
//!
//! Path: `$OPENDECK_M18_LED_SOCKET`, else `<temp dir>/opendeck-m18-leds.sock`.
//! OpenDeck starts every plugin with the same environment, so another plugin
//! resolves the same temp dir.

use std::{collections::HashMap, os::unix::fs::PermissionsExt, path::PathBuf, sync::LazyLock};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

use crate::{
    SESSIONS,
    device::{ASLEEP, DeviceCommand, LED_LEVEL},
    palette::{self, LED_COUNT, LedPalette},
};

/// The palette each device is showing now: key-chosen or live.
static LIVE: LazyLock<Mutex<HashMap<String, LedPalette>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn socket_path() -> PathBuf {
    std::env::var_os("OPENDECK_M18_LED_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("opendeck-m18-leds.sock"))
}

/// Called whenever the Set LED Colors key (or a restore) applies a palette, so
/// `set` requests layer on top of what is actually lit.
pub async fn note_applied(device_id: &str, colors: LedPalette) {
    LIVE.lock().await.insert(device_id.to_owned(), colors);
}

pub async fn serve(token: std::sync::Arc<CancellationToken>) {
    let path = socket_path();
    // A previous run's socket file blocks bind; nothing can be listening on it
    // because OpenDeck runs one instance of this plugin.
    let _ = std::fs::remove_file(&path);
    let listener = match UnixListener::bind(&path) {
        Ok(listener) => listener,
        Err(error) => {
            log::error!("LED socket: cannot bind {}: {}", path.display(), error);
            return;
        }
    };
    if let Err(error) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)) {
        log::warn!("LED socket: cannot restrict permissions: {}", error);
    }
    log::info!("LED socket listening on {}", path.display());

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => { tokio::spawn(handle(stream)); }
                Err(error) => log::warn!("LED socket: accept failed: {}", error),
            },
        }
    }
    let _ = std::fs::remove_file(&path);
}

async fn handle(stream: UnixStream) {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Value>(&line) {
            Ok(request) => apply(&request).await.unwrap_or_else(|error| json!({ "error": error })),
            Err(error) => json!({ "error": format!("bad JSON: {error}") }),
        };
        if write.write_all(format!("{reply}\n").as_bytes()).await.is_err() {
            break;
        }
    }
}

#[derive(Clone, Copy)]
enum Control {
    Brightness(u8),
    Standby(bool),
}

async fn apply(request: &Value) -> Result<Value, String> {
    let devices: Vec<String> = match request.get("device").and_then(Value::as_str) {
        Some(id) => vec![id.to_owned()],
        None => SESSIONS.read().await.output_ids(),
    };
    if devices.is_empty() {
        return Err("no M18 connected".into());
    }

    if request.get("get").and_then(Value::as_bool) == Some(true) {
        let live = LIVE.lock().await;
        let palettes: serde_json::Map<String, Value> = devices
            .iter()
            .filter_map(|id| live.get(id).map(|p| (id.clone(), palette::action_settings(p)["ledColors"].clone())))
            .collect();
        let level = LED_LEVEL.load(std::sync::atomic::Ordering::Acquire);
        let asleep: Vec<String> = ASLEEP.lock().unwrap().iter().cloned().collect();
        return Ok(json!({ "ok": true, "palettes": palettes, "brightness": (level <= 100).then_some(level), "asleep": asleep }));
    }

    // Brightness and standby go straight to each device's output worker.
    let control = if let Some(level) = request.get("brightness") {
        let level = level.as_u64().filter(|l| *l <= 100).ok_or("brightness must be 0-100")? as u8;
        LED_LEVEL.store(level, std::sync::atomic::Ordering::Release);
        Some(Control::Brightness(level))
    } else {
        request.get("standby").and_then(Value::as_bool).map(Control::Standby)
    };
    if let Some(control) = control {
        for id in &devices {
            let output = SESSIONS.read().await.output(id).cloned();
            let Some(output) = output else {
                return Err(format!("unknown device {id}"));
            };
            let command = match control {
                Control::Brightness(level) => DeviceCommand::SetLedBrightness(level),
                Control::Standby(sleep) => DeviceCommand::Standby(sleep),
            };
            if output.send(command).await.is_err() {
                output.token.cancel();
                return Err(format!("device {id} is unavailable"));
            }
        }
        return Ok(json!({ "ok": true, "devices": devices }));
    }

    let mut applied = Vec::new();
    for id in devices {
        let next = next_palette(&id, request).await?;
        let output = SESSIONS.read().await.output(&id).cloned();
        let Some(output) = output else {
            return Err(format!("unknown device {id}"));
        };
        if output.send(DeviceCommand::SetLedColors(next)).await.is_err() {
            output.token.cancel();
            return Err(format!("device {id} is unavailable"));
        }
        LIVE.lock().await.insert(id.clone(), next);
        applied.push(id);
    }
    Ok(json!({ "ok": true, "devices": applied }))
}

async fn next_palette(id: &str, request: &Value) -> Result<LedPalette, String> {
    if request.get("restore").and_then(Value::as_bool) == Some(true) {
        return palette::PALETTES
            .lock()
            .await
            .get(id)
            .ok_or_else(|| format!("no saved palette for {id}"));
    }
    if let Some(colors) = request.get("colors") {
        return palette::parse_palette(&json!({ "ledColors": colors }))
            .ok_or_else(|| format!("colors must be {LED_COUNT} \"#rrggbb\" strings"));
    }
    if let Some(set) = request.get("set").and_then(Value::as_object) {
        let base = match LIVE.lock().await.get(id).copied() {
            Some(live) => live,
            None => palette::PALETTES.lock().await.get(id).unwrap_or(palette::DEFAULT_PALETTE),
        };
        let mut next = base;
        for (index, color) in set {
            let i: usize = index.parse().map_err(|_| format!("LED index {index:?} is not a number"))?;
            if i >= LED_COUNT {
                return Err(format!("LED index {i} out of range 0..{LED_COUNT}"));
            }
            next[i] = color
                .as_str()
                .and_then(palette::parse_color)
                .ok_or_else(|| format!("LED {i}: colour must be \"#rrggbb\""))?;
        }
        return Ok(next);
    }
    Err("expected one of colors, set, restore, get".into())
}
