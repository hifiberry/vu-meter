use pipewire as pw;
use pw::spa::param::audio::{AudioFormat, AudioInfoRaw};
use pw::spa::pod::Pod;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::decibel;

/// Per-channel level data, ready for binary encoding
#[derive(Debug, Clone, Copy, Default)]
pub struct ChannelLevels {
    pub rms_u8: u8,
    pub peak_u8: u8,
    pub clipping: bool,
}

/// Shared state between the PipeWire capture thread and the WebSocket server
pub struct MeterState {
    pub channels: Vec<ChannelLevels>,
}

impl Default for MeterState {
    fn default() -> Self {
        Self {
            channels: vec![ChannelLevels::default(); 2],
        }
    }
}

const MIN_DB: f64 = -60.0;
const MAX_DB: f64 = 0.0;
const SAMPLE_RATE: u32 = 48000;
const NUM_CHANNELS: usize = 2;
const UPDATE_INTERVAL_MS: u64 = 100;
const MAX_VALUE_S32: f64 = 2147483648.0;

/// Start the PipeWire capture. Returns the shared meter state, quit flag, and client counter.
pub fn start_capture(
    target: Option<String>,
) -> (Arc<Mutex<MeterState>>, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let state = Arc::new(Mutex::new(MeterState::default()));
    let quit = Arc::new(AtomicBool::new(false));
    let clients = Arc::new(AtomicUsize::new(0));

    let buffer: Arc<Mutex<Vec<Vec<i32>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); NUM_CHANNELS]));

    // Spawn PipeWire capture thread (uses main_loop.run(), blocks)
    let buffer_for_pw = buffer.clone();
    let target_clone = target.clone();
    thread::spawn(move || {
        run_pipewire_loop(target_clone, buffer_for_pw);
    });

    // Spawn processing thread that computes levels from the buffer
    let state_for_proc = state.clone();
    let quit_for_proc = quit.clone();
    let clients_for_proc = clients.clone();
    thread::spawn(move || {
        let frames_per_update =
            (SAMPLE_RATE as f64 * UPDATE_INTERVAL_MS as f64 / 1000.0) as usize;

        loop {
            if quit_for_proc.load(Ordering::Relaxed) {
                break;
            }

            thread::sleep(Duration::from_millis(UPDATE_INTERVAL_MS));

            // Only process when clients are connected
            if clients_for_proc.load(Ordering::Relaxed) == 0 {
                // Drain buffer to prevent unbounded growth while idle
                if let Ok(mut buf) = buffer.lock() {
                    for ch in buf.iter_mut() {
                        ch.clear();
                    }
                }
                // Reset meter state to zeros
                if let Ok(mut s) = state_for_proc.lock() {
                    s.channels = vec![ChannelLevels::default(); NUM_CHANNELS];
                }
                continue;
            }

            let mut buf = buffer.lock().unwrap();
            let mut levels = vec![ChannelLevels::default(); NUM_CHANNELS];

            for ch in 0..NUM_CHANNELS {
                if buf[ch].len() >= frames_per_update {
                    let samples: Vec<i32> = buf[ch].drain(..frames_per_update).collect();
                    let rms_db =
                        decibel::calculate_rms_db(&samples, MAX_VALUE_S32, MIN_DB, MAX_DB);
                    let peak_db =
                        decibel::calculate_peak_db(&samples, MAX_VALUE_S32, MIN_DB, MAX_DB);
                    let clipping = decibel::detect_clipping(&samples, MAX_VALUE_S32);

                    levels[ch] = ChannelLevels {
                        rms_u8: decibel::db_to_u8(rms_db, MIN_DB, MAX_DB),
                        peak_u8: decibel::db_to_u8(peak_db, MIN_DB, MAX_DB),
                        clipping,
                    };
                } else {
                    // Not enough data — clear to prevent unbounded growth
                    buf[ch].clear();
                }
            }

            if let Ok(mut s) = state_for_proc.lock() {
                s.channels = levels;
            }
        }
    });

    // If a specific target is given, link via pw-link after stream is ready
    if let Some(ref t) = target {
        let target_name = t.clone();
        thread::spawn(move || {
            for _ in 0..30 {
                thread::sleep(Duration::from_millis(100));
                if let Ok(output) = std::process::Command::new("pw-link")
                    .arg("-i")
                    .output()
                {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    if stdout.contains("vu-meter-capture:input_FL") {
                        let _ = std::process::Command::new("pw-link")
                            .arg(format!("{}:capture_FL", target_name))
                            .arg("vu-meter-capture:input_FL")
                            .output();
                        let _ = std::process::Command::new("pw-link")
                            .arg(format!("{}:capture_FR", target_name))
                            .arg("vu-meter-capture:input_FR")
                            .output();
                        break;
                    }
                }
            }
        });
    }

    (state, quit, clients)
}

fn run_pipewire_loop(
    target: Option<String>,
    buffer: Arc<Mutex<Vec<Vec<i32>>>>,
) {
    pw::init();

    let main_loop = match pw::main_loop::MainLoop::new(None) {
        Ok(ml) => ml,
        Err(e) => {
            eprintln!("Failed to create PipeWire main loop: {:?}", e);
            return;
        }
    };

    let context = match pw::context::Context::new(&main_loop) {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Failed to create PipeWire context: {:?}", e);
            return;
        }
    };

    let core = match context.connect(None) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to PipeWire: {:?}", e);
            return;
        }
    };

    let mut audio_info = AudioInfoRaw::new();
    audio_info.set_format(AudioFormat::S32LE);
    audio_info.set_rate(SAMPLE_RATE);
    audio_info.set_channels(NUM_CHANNELS as u32);

    let use_autoconnect = target.is_none();

    let stream = match pw::stream::Stream::new(
        &core,
        "vu-meter-capture",
        pw::properties::properties! {
            *pw::keys::MEDIA_TYPE => "Audio",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Music",
            *pw::keys::NODE_NAME => "vu-meter-capture",
        },
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to create PipeWire stream: {:?}", e);
            return;
        }
    };

    let _listener = stream
        .add_local_listener_with_user_data(())
        .process(move |stream, _| {
            if let Some(mut pw_buf) = stream.dequeue_buffer() {
                let datas = pw_buf.datas_mut();
                if let Some(data) = datas.first_mut() {
                    let chunk = data.chunk();
                    let size = chunk.size() as usize;
                    if let Some(slice) = data.data() {
                        let frame_size = 4 * NUM_CHANNELS; // S32LE
                        let num_frames = size / frame_size;
                        let mut buf = buffer.lock().unwrap();
                        if buf.is_empty() {
                            *buf = vec![Vec::new(); NUM_CHANNELS];
                        }
                        for frame in 0..num_frames {
                            for ch in 0..NUM_CHANNELS {
                                let offset = frame * frame_size + ch * 4;
                                if offset + 4 <= slice.len() {
                                    let sample = i32::from_le_bytes([
                                        slice[offset],
                                        slice[offset + 1],
                                        slice[offset + 2],
                                        slice[offset + 3],
                                    ]);
                                    buf[ch].push(sample);
                                }
                            }
                        }
                    }
                }
            }
        })
        .register();

    if _listener.is_err() {
        eprintln!("Failed to register stream listener");
        return;
    }

    // Build format parameter
    let obj = pw::spa::pod::Object {
        type_: pw::spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
        id: pw::spa::param::ParamType::EnumFormat.as_raw(),
        properties: audio_info.into(),
    };
    let values: Vec<u8> = match pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(obj),
    ) {
        Ok((cursor, _)) => cursor.into_inner(),
        Err(e) => {
            eprintln!("Failed to serialize audio info: {:?}", e);
            return;
        }
    };

    let mut params = [Pod::from_bytes(&values).unwrap()];

    let stream_flags = if use_autoconnect {
        pw::stream::StreamFlags::AUTOCONNECT
            | pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS
    } else {
        pw::stream::StreamFlags::MAP_BUFFERS | pw::stream::StreamFlags::RT_PROCESS
    };

    if let Err(e) = stream.connect(
        pw::spa::utils::Direction::Input,
        None,
        stream_flags,
        &mut params,
    ) {
        eprintln!("Failed to connect PipeWire stream: {:?}", e);
        return;
    }

    // Run the main loop (blocks until quit)
    main_loop.run();
}
