#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod audio;

use audio::{store_db, DeviceLists, Engine, NBANDS};
use serde::Serialize;
use std::sync::atomic::Ordering;

struct AppState {
    engine: Engine,
}

#[derive(Serialize)]
struct Meters {
    peak_in: f32,
    peak_out: f32,
    clipping: bool,
    running: bool,
    underruns: u32,
    bands: Vec<f32>,
}

#[tauri::command]
fn list_devices() -> DeviceLists {
    audio::list_devices()
}

#[tauri::command]
fn start(
    state: tauri::State<AppState>,
    input: String,
    output: String,
    delay_ms: u32,
) -> Result<(), String> {
    state
        .engine
        .shared
        .delay_ms
        .store(delay_ms.clamp(20, 4000), Ordering::Relaxed);
    state.engine.start(input, output)
}

#[tauri::command]
fn stop(state: tauri::State<AppState>) {
    state.engine.stop();
}

#[tauri::command]
fn set_delay(state: tauri::State<AppState>, delay_ms: u32) {
    state
        .engine
        .shared
        .delay_ms
        .store(delay_ms.clamp(20, 4000), Ordering::Relaxed);
}

#[tauri::command]
fn set_gains(state: tauri::State<AppState>, mic_db: f32, out_db: f32) {
    state
        .engine
        .shared
        .mic_gain
        .store(store_db(mic_db), Ordering::Relaxed);
    state
        .engine
        .shared
        .out_gain
        .store(store_db(out_db), Ordering::Relaxed);
}

#[tauri::command]
fn meters(state: tauri::State<AppState>) -> Meters {
    let s = &state.engine.shared;
    let clipping = s.clipping.swap(false, Ordering::Relaxed);
    let mut bands = Vec::with_capacity(NBANDS);
    for i in 0..NBANDS {
        bands.push(f32::from_bits(s.bands[i].load(Ordering::Relaxed)));
    }
    Meters {
        peak_in: f32::from_bits(s.peak_in.load(Ordering::Relaxed)),
        peak_out: f32::from_bits(s.peak_out.load(Ordering::Relaxed)),
        clipping,
        running: s.running.load(Ordering::Relaxed),
        underruns: s.underruns.swap(0, Ordering::Relaxed),
        bands,
    }
}

#[tauri::command]
fn measure(state: tauri::State<AppState>, input: String, output: String) -> Result<f64, String> {
    state.engine.measure(input, output)
}

// ---------------------------------------------------------------- настройки на диске

fn settings_path() -> Option<std::path::PathBuf> {
    let base = std::env::var("APPDATA").ok()?;
    let dir = std::path::Path::new(&base).join("daf-trainer");
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir.join("settings.json"))
}

#[tauri::command]
fn load_settings() -> String {
    settings_path()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .unwrap_or_else(|| "{}".to_string())
}

#[tauri::command]
fn save_settings(json: String) -> Result<(), String> {
    let path = settings_path().ok_or("не удалось определить папку настроек")?;
    std::fs::write(path, json).map_err(|e| e.to_string())
}

fn main() {
    tauri::Builder::default()
        .manage(AppState {
            engine: Engine::new(),
        })
        .invoke_handler(tauri::generate_handler![
            list_devices,
            start,
            stop,
            set_delay,
            set_gains,
            meters,
            measure,
            load_settings,
            save_settings
        ])
        .run(tauri::generate_context!())
        .expect("не удалось запустить приложение");
}
