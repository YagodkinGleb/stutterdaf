// Звуковое ядро DAF Trainer.
//
// Устройство работы: cpal-потоки живут в отдельном рабочем потоке, потому что
// cpal::Stream нельзя передавать между потоками. Управление приходит по каналу,
// параметры и показания читаются через атомарные переменные без блокировок —
// в аудио-колбэках блокировки использовать нельзя, они вызывают щелчки.

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{BufferSize, Device, SampleFormat, StreamConfig};
use ringbuf::HeapRb;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SAMPLE_RATE: u32 = 48_000;
/// Насколько быстро подтягиваем фактическую задержку к заданной, семплов за колбэк.
/// Больше — быстрее реакция ползунка, но заметнее уплывающий тон в момент перехода.
const SLEW_PER_CALLBACK: usize = 128;
/// Мёртвая зона: длина очереди всё время гуляет на размер буфера,
/// и без этого порога ядро подстраивало бы её непрерывно — отсюда дрожание тона.
const SLEW_DEADBAND: i64 = 240;
/// Минимальная задержка. Короче одного звукового буфера очередь пустеет,
/// в звуке появляются провалы.
const MIN_DELAY_MS: u32 = 20;

// ---------------------------------------------------------------- общее состояние

pub struct Shared {
    pub delay_ms: AtomicU32,
    /// Усиление в децибелах, умноженное на 10 и смещённое на +1000,
    /// чтобы хранить со знаком в беззнаковом атомике.
    pub mic_gain: AtomicU32,
    pub out_gain: AtomicU32,
    pub peak_in: AtomicU32,  // биты f32
    pub peak_out: AtomicU32, // биты f32
    pub clipping: AtomicBool,
    pub running: AtomicBool,
    pub underruns: AtomicU32,
    /// Энергия по восьми полосам, биты f32. Нужна визуализации:
    /// низкие частоты дают крупные лепестки, высокие дробят рисунок.
    pub bands: [AtomicU32; NBANDS],
}

pub const NBANDS: usize = 8;
/// Центры полос в герцах: от основного тона голоса до шипящих.
pub const BAND_HZ: [f32; NBANDS] = [110.0, 220.0, 420.0, 800.0, 1500.0, 2600.0, 4200.0, 6800.0];

/// Полосовой фильтр Чемберлина. Дёшев настолько, что восемь штук
/// на семпл не заметны на фоне остальной работы колбэка.
#[derive(Clone, Copy)]
struct Svf {
    f: f32,
    q: f32,
    low: f32,
    band: f32,
}

impl Svf {
    fn new(hz: f32) -> Svf {
        let f = 2.0 * (std::f32::consts::PI * hz / SAMPLE_RATE as f32).sin();
        Svf {
            f: f.min(0.9),
            q: 0.30,
            low: 0.0,
            band: 0.0,
        }
    }
    #[inline]
    fn run(&mut self, x: f32) -> f32 {
        self.low += self.f * self.band;
        let high = x - self.low - self.q * self.band;
        self.band += self.f * high;
        self.band
    }
}

impl Shared {
    fn new() -> Self {
        Shared {
            delay_ms: AtomicU32::new(120),
            mic_gain: AtomicU32::new(1000),
            out_gain: AtomicU32::new(1000),
            peak_in: AtomicU32::new(0),
            peak_out: AtomicU32::new(0),
            clipping: AtomicBool::new(false),
            running: AtomicBool::new(false),
            underruns: AtomicU32::new(0),
            bands: Default::default(),
        }
    }
}

fn db_to_lin(stored: u32) -> f32 {
    let db = (stored as f32 - 1000.0) / 10.0;
    10f32.powf(db / 20.0)
}

pub fn store_db(db: f32) -> u32 {
    ((db * 10.0) + 1000.0).clamp(0.0, 2000.0) as u32
}

// ---------------------------------------------------------------- команды

enum Cmd {
    Start {
        input: String,
        output: String,
        reply: Sender<Result<(), String>>,
    },
    Stop,
    Measure {
        input: String,
        output: String,
        reply: Sender<Result<f64, String>>,
    },
}

pub struct Engine {
    tx: Mutex<Sender<Cmd>>,
    pub shared: Arc<Shared>,
}

impl Engine {
    pub fn new() -> Engine {
        let shared = Arc::new(Shared::new());
        let (tx, rx) = channel::<Cmd>();
        let worker_shared = shared.clone();
        std::thread::spawn(move || worker(rx, worker_shared));
        Engine {
            tx: Mutex::new(tx),
            shared,
        }
    }

    pub fn start(&self, input: String, output: String) -> Result<(), String> {
        let (reply, wait) = channel();
        self.tx
            .lock()
            .map_err(|_| "звуковой поток недоступен".to_string())?
            .send(Cmd::Start {
                input,
                output,
                reply,
            })
            .map_err(|_| "звуковой поток недоступен".to_string())?;
        wait.recv().map_err(|_| "нет ответа от ядра".to_string())?
    }

    pub fn stop(&self) {
        if let Ok(tx) = self.tx.lock() {
            let _ = tx.send(Cmd::Stop);
        }
    }

    pub fn measure(&self, input: String, output: String) -> Result<f64, String> {
        let (reply, wait) = channel();
        self.tx
            .lock()
            .map_err(|_| "звуковой поток недоступен".to_string())?
            .send(Cmd::Measure {
                input,
                output,
                reply,
            })
            .map_err(|_| "звуковой поток недоступен".to_string())?;
        wait.recv()
            .map_err(|_| "нет ответа от ядра".to_string())?
    }
}

// ---------------------------------------------------------------- рабочий поток

fn worker(rx: Receiver<Cmd>, shared: Arc<Shared>) {
    // Потоки держим здесь: пока переменная жива — звук идёт.
    let mut streams: Option<(cpal::Stream, cpal::Stream)> = None;

    while let Ok(cmd) = rx.recv() {
        match cmd {
            Cmd::Start {
                input,
                output,
                reply,
            } => {
                streams = None;
                shared.running.store(false, Ordering::Relaxed);
                match build_daf(&input, &output, &shared) {
                    Ok(s) => {
                        streams = Some(s);
                        shared.running.store(true, Ordering::Relaxed);
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Cmd::Stop => {
                streams = None;
                shared.running.store(false, Ordering::Relaxed);
                shared.peak_in.store(0, Ordering::Relaxed);
                shared.peak_out.store(0, Ordering::Relaxed);
            }
            Cmd::Measure {
                input,
                output,
                reply,
            } => {
                let was_running = streams.is_some();
                streams = None;
                shared.running.store(false, Ordering::Relaxed);

                let result = measure_roundtrip(&input, &output);
                let _ = reply.send(result);

                if was_running {
                    if let Ok(s) = build_daf(&input, &output, &shared) {
                        streams = Some(s);
                        shared.running.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------- список устройств

#[derive(Serialize)]
pub struct DeviceInfo {
    pub name: String,
    pub rate: u32,
}

#[derive(Serialize)]
pub struct DeviceLists {
    pub inputs: Vec<DeviceInfo>,
    pub outputs: Vec<DeviceInfo>,
}

pub fn list_devices() -> DeviceLists {
    let host = cpal::default_host();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();

    if let Ok(devs) = host.input_devices() {
        for d in devs {
            if let Ok(name) = d.name() {
                let rate = d
                    .default_input_config()
                    .map(|c| c.sample_rate().0)
                    .unwrap_or(0);
                inputs.push(DeviceInfo { name, rate });
            }
        }
    }
    if let Ok(devs) = host.output_devices() {
        for d in devs {
            if let Ok(name) = d.name() {
                let rate = d
                    .default_output_config()
                    .map(|c| c.sample_rate().0)
                    .unwrap_or(0);
                outputs.push(DeviceInfo { name, rate });
            }
        }
    }
    DeviceLists { inputs, outputs }
}

fn find_device(name: &str, is_input: bool) -> Option<Device> {
    let host = cpal::default_host();
    let devs: Vec<Device> = if is_input {
        host.input_devices().ok()?.collect()
    } else {
        host.output_devices().ok()?.collect()
    };
    devs.into_iter()
        .find(|d| d.name().map(|n| n == name).unwrap_or(false))
}

fn config_for(device: &Device, is_input: bool) -> Option<(StreamConfig, u16)> {
    let supported: Vec<_> = if is_input {
        device.supported_input_configs().ok()?.collect()
    } else {
        device.supported_output_configs().ok()?.collect()
    };

    let mut chosen: Option<u16> = None;
    for c in &supported {
        if c.sample_format() != SampleFormat::F32 {
            continue;
        }
        if c.min_sample_rate().0 <= SAMPLE_RATE && SAMPLE_RATE <= c.max_sample_rate().0 {
            let ch = c.channels();
            if chosen.map_or(true, |best| ch < best) {
                chosen = Some(ch);
            }
        }
    }
    let channels = chosen?;
    Some((
        StreamConfig {
            channels,
            sample_rate: cpal::SampleRate(SAMPLE_RATE),
            buffer_size: BufferSize::Default,
        },
        channels,
    ))
}

// ---------------------------------------------------------------- сам DAF

fn build_daf(
    input_name: &str,
    output_name: &str,
    shared: &Arc<Shared>,
) -> Result<(cpal::Stream, cpal::Stream), String> {
    let input = find_device(input_name, true).ok_or("микрофон не найден")?;
    let output = find_device(output_name, false).ok_or("устройство вывода не найдено")?;

    let (in_cfg, in_ch) = config_for(&input, true).ok_or("микрофон не отдаёт 48 кГц")?;
    let (out_cfg, out_ch) = config_for(&output, false).ok_or("выход не отдаёт 48 кГц")?;

    // Линия задержки на 5 секунд с запасом.
    let rb = HeapRb::<f32>::new(SAMPLE_RATE as usize * 5);
    let (mut prod, mut cons) = rb.split();

    let in_shared = shared.clone();
    // Состояние фильтров живёт между вызовами колбэка.
    let mut filters: [Svf; NBANDS] = std::array::from_fn(|i| Svf::new(BAND_HZ[i]));
    let mut band_env = [0.0f32; NBANDS];

    let in_stream = input
        .build_input_stream(
            &in_cfg,
            move |data: &[f32], _: &cpal::InputCallbackInfo| {
                let gain = db_to_lin(in_shared.mic_gain.load(Ordering::Relaxed));
                let mut peak = 0.0f32;
                let mut clipped = false;
                let mut band_peak = [0.0f32; NBANDS];

                for frame in data.chunks(in_ch as usize) {
                    let mut mono = frame.iter().sum::<f32>() / in_ch as f32;
                    mono *= gain;
                    if mono > 1.0 || mono < -1.0 {
                        clipped = true;
                    }
                    let mono = mono.clamp(-1.0, 1.0);
                    peak = peak.max(mono.abs());

                    for i in 0..NBANDS {
                        let v = filters[i].run(mono).abs();
                        if v > band_peak[i] {
                            band_peak[i] = v;
                        }
                    }

                    let _ = prod.push(mono);
                }

                // Сглаживание: резкий подъём, мягкий спад — иначе картинка дрожит.
                for i in 0..NBANDS {
                    // высокие полосы тише по природе речи, поднимаем их наклоном
                    let tilt = 1.0 + i as f32 * 0.45;
                    let target = (band_peak[i] * tilt).min(1.0);
                    let k = if target > band_env[i] { 0.6 } else { 0.08 };
                    band_env[i] += k * (target - band_env[i]);
                    in_shared.bands[i].store(band_env[i].to_bits(), Ordering::Relaxed);
                }

                in_shared.peak_in.store(peak.to_bits(), Ordering::Relaxed);
                if clipped {
                    in_shared.clipping.store(true, Ordering::Relaxed);
                }
            },
            |e| eprintln!("вход: {e}"),
            None,
        )
        .map_err(|e| format!("не удалось открыть микрофон: {e}"))?;

    let out_shared = shared.clone();
    let out_stream = output
        .build_output_stream(
            &out_cfg,
            move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                let gain = db_to_lin(out_shared.out_gain.load(Ordering::Relaxed));
                let target = (out_shared
                    .delay_ms
                    .load(Ordering::Relaxed)
                    .max(MIN_DELAY_MS) as usize)
                    * SAMPLE_RATE as usize
                    / 1000;

                let have = cons.len();
                // Разница между желаемой и фактической задержкой.
                // Положительная — нужно добавить тишины, отрицательная — выкинуть лишнее.
                let raw_deficit = target as i64 - have as i64;
                // В пределах мёртвой зоны ничего не трогаем: очередь и так дышит.
                let mut deficit = if raw_deficit.abs() > SLEW_DEADBAND {
                    raw_deficit
                } else {
                    0
                };
                let mut budget = SLEW_PER_CALLBACK as i64;
                let mut peak = 0.0f32;
                let mut underruns = 0u32;

                for frame in data.chunks_mut(out_ch as usize) {
                    let v = if deficit > 0 && budget > 0 {
                        // растим задержку: вставляем тишину, ничего не вычитывая
                        deficit -= 1;
                        budget -= 1;
                        0.0
                    } else {
                        if deficit < 0 && budget > 0 {
                            // сокращаем задержку: пропускаем лишний семпл
                            let _ = cons.pop();
                            deficit += 1;
                            budget -= 1;
                        }
                        match cons.pop() {
                            Some(v) => v,
                            None => {
                                underruns += 1;
                                0.0
                            }
                        }
                    };

                    let v = (v * gain).clamp(-1.0, 1.0);
                    peak = peak.max(v.abs());
                    for s in frame.iter_mut() {
                        *s = v;
                    }
                }

                out_shared.peak_out.store(peak.to_bits(), Ordering::Relaxed);
                if underruns > 0 {
                    out_shared
                        .underruns
                        .fetch_add(underruns, Ordering::Relaxed);
                }
            },
            |e| eprintln!("выход: {e}"),
            None,
        )
        .map_err(|e| format!("не удалось открыть выход: {e}"))?;

    in_stream
        .play()
        .map_err(|e| format!("микрофон не запустился: {e}"))?;
    out_stream
        .play()
        .map_err(|e| format!("выход не запустился: {e}"))?;

    Ok((in_stream, out_stream))
}

// ---------------------------------------------------------------- калибровка

/// Проигрывает короткие щелчки и ловит их микрофоном.
/// Возвращает медиану полного круга в миллисекундах.
fn measure_roundtrip(input_name: &str, output_name: &str) -> Result<f64, String> {
    let input = find_device(input_name, true).ok_or("микрофон не найден")?;
    let output = find_device(output_name, false).ok_or("устройство вывода не найдено")?;

    let (in_cfg, in_ch) = config_for(&input, true).ok_or("микрофон не отдаёт 48 кГц")?;
    let (out_cfg, out_ch) = config_for(&output, false).ok_or("выход не отдаёт 48 кГц")?;

    let armed = Arc::new(AtomicBool::new(false));
    let fired = Arc::new(AtomicBool::new(false));
    let click_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let results: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let noise: Arc<Mutex<f32>> = Arc::new(Mutex::new(0.0));
    let calibrating = Arc::new(AtomicBool::new(true));

    let click_len = (SAMPLE_RATE / 500) as usize; // ~2 мс
    let out_armed = armed.clone();
    let out_fired = fired.clone();
    let out_click_at = click_at.clone();
    let mut click_pos = 0usize;

    let out_stream = output
        .build_output_stream(
            &out_cfg,
            move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
                for frame in data.chunks_mut(out_ch as usize) {
                    let mut v = 0.0f32;
                    if out_armed.load(Ordering::Relaxed) {
                        if click_pos < click_len {
                            if click_pos == 0 {
                                if let Ok(mut g) = out_click_at.lock() {
                                    *g = Some(Instant::now());
                                }
                                out_fired.store(true, Ordering::Relaxed);
                            }
                            let t = click_pos as f32 / SAMPLE_RATE as f32;
                            v = (t * 1000.0 * std::f32::consts::TAU).sin() * 0.7;
                            click_pos += 1;
                        } else {
                            out_armed.store(false, Ordering::Relaxed);
                            click_pos = 0;
                        }
                    }
                    for s in frame.iter_mut() {
                        *s = v;
                    }
                }
            },
            |e| eprintln!("калибровка, выход: {e}"),
            None,
        )
        .map_err(|e| format!("не удалось открыть выход: {e}"))?;

    let in_click_at = click_at.clone();
    let in_fired = fired.clone();
    let in_results = results.clone();
    let in_noise = noise.clone();
    let in_calibrating = calibrating.clone();

    let in_stream = input
        .build_input_stream(
            &in_cfg,
            move |data: &[f32], _: &cpal::InputCallbackInfo| {
                let now = Instant::now();
                let frames: Vec<f32> = data
                    .chunks(in_ch as usize)
                    .map(|f| f.iter().sum::<f32>() / in_ch as f32)
                    .collect();

                if in_calibrating.load(Ordering::Relaxed) {
                    let peak = frames.iter().fold(0.0f32, |a, b| a.max(b.abs()));
                    if let Ok(mut n) = in_noise.lock() {
                        *n = n.max(peak);
                    }
                    return;
                }
                if !in_fired.load(Ordering::Relaxed) {
                    return;
                }

                let floor = in_noise.lock().map(|n| *n).unwrap_or(0.01);
                let threshold = (floor * 8.0).max(0.02);

                for (i, s) in frames.iter().enumerate() {
                    if s.abs() > threshold {
                        if let Ok(mut guard) = in_click_at.lock() {
                            if let Some(t0) = guard.take() {
                                let tail = (frames.len() - i) as f64 / SAMPLE_RATE as f64;
                                let detect = now - Duration::from_secs_f64(tail);
                                let ms = detect.duration_since(t0).as_secs_f64() * 1000.0;
                                if ms > 0.5 && ms < 1500.0 {
                                    if let Ok(mut r) = in_results.lock() {
                                        r.push(ms);
                                    }
                                }
                                in_fired.store(false, Ordering::Relaxed);
                            }
                        }
                        break;
                    }
                }
            },
            |e| eprintln!("калибровка, вход: {e}"),
            None,
        )
        .map_err(|e| format!("не удалось открыть микрофон: {e}"))?;

    in_stream.play().map_err(|e| format!("вход: {e}"))?;
    out_stream.play().map_err(|e| format!("выход: {e}"))?;

    std::thread::sleep(Duration::from_millis(700));
    calibrating.store(false, Ordering::Relaxed);

    for _ in 0..5 {
        if let Ok(mut g) = click_at.lock() {
            *g = None;
        }
        fired.store(false, Ordering::Relaxed);
        armed.store(true, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(700));
    }

    drop(in_stream);
    drop(out_stream);

    let mut r = results.lock().map(|r| r.clone()).unwrap_or_default();
    if r.is_empty() {
        return Err("щелчок не пойман: микрофон не слышит наушники".into());
    }
    r.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(r[r.len() / 2])
}
