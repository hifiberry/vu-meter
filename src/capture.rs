use pipewire as pw;
use pw::spa::param::audio::{AudioFormat, AudioInfoRaw};
use pw::spa::pod::Pod;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
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
const FRAMES_PER_UPDATE: usize = (SAMPLE_RATE as usize * UPDATE_INTERVAL_MS as usize) / 1000;
/// Hard cap on frames buffered per channel. Without this, a processing
/// thread stall would let the buffer grow with every incoming sample, and
/// nothing ever gives that capacity back -- the peak size becomes the
/// permanent floor. Capping it keeps memory bounded regardless of how long
/// a stall lasts; the cost is evicting the oldest buffered samples during
/// that stall, not a growing process.
const MAX_BUFFERED_FRAMES: usize = FRAMES_PER_UPDATE * 2;

/// Push a sample, evicting the oldest one first once the channel is at its
/// bound -- a live meter should reflect what's playing now, not stale audio
/// kept around from before an overload. Returns whether an eviction
/// happened, so callers can count it. Extracted so the bound itself is
/// unit-testable without a PipeWire stream.
fn push_bounded(buf: &mut VecDeque<i32>, sample: i32, cap: usize) -> bool {
    let evicted = if buf.len() >= cap {
        buf.pop_front();
        true
    } else {
        false
    };
    buf.push_back(sample);
    evicted
}

fn new_channel_buffers() -> Vec<VecDeque<i32>> {
    (0..NUM_CHANNELS)
        .map(|_| VecDeque::with_capacity(MAX_BUFFERED_FRAMES))
        .collect()
}

/// Start the PipeWire capture. Returns the shared meter state, quit flag, and client counter.
pub fn start_capture(
    target: Option<String>,
) -> (Arc<Mutex<MeterState>>, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let state = Arc::new(Mutex::new(MeterState::default()));
    let quit = Arc::new(AtomicBool::new(false));
    let clients = Arc::new(AtomicUsize::new(0));

    let buffer: Arc<Mutex<Vec<VecDeque<i32>>>> = Arc::new(Mutex::new(new_channel_buffers()));

    // Counts of periods the realtime callback dropped outright (lock
    // contended) and samples it overwrote at the bound (channel stayed
    // full). Both are incremented only with atomics, so bumping them from
    // the realtime thread is itself realtime-safe; the processing thread
    // logs them periodically so an overload is visible instead of silent.
    let dropped_periods = Arc::new(AtomicUsize::new(0));
    let dropped_samples = Arc::new(AtomicUsize::new(0));

    // Spawn PipeWire capture thread (uses main_loop.run(), blocks)
    let buffer_for_pw = buffer.clone();
    let target_clone = target.clone();
    let dropped_periods_pw = dropped_periods.clone();
    let dropped_samples_pw = dropped_samples.clone();
    thread::spawn(move || {
        run_pipewire_loop(
            target_clone,
            buffer_for_pw,
            dropped_periods_pw,
            dropped_samples_pw,
        );
    });

    // Spawn processing thread that computes levels from the buffer
    let state_for_proc = state.clone();
    let quit_for_proc = quit.clone();
    let clients_for_proc = clients.clone();
    thread::spawn(move || {
        loop {
            if quit_for_proc.load(Ordering::Relaxed) {
                break;
            }

            thread::sleep(Duration::from_millis(UPDATE_INTERVAL_MS));

            let periods = dropped_periods.swap(0, Ordering::Relaxed);
            let samples = dropped_samples.swap(0, Ordering::Relaxed);
            if periods > 0 || samples > 0 {
                eprintln!(
                    "vu-meter: capture overloaded in the last {}ms: {} periods dropped, {} samples evicted",
                    UPDATE_INTERVAL_MS, periods, samples
                );
            }

            // Only process when clients are connected
            if clients_for_proc.load(Ordering::Relaxed) == 0 {
                // Drain buffer to discard stale data while idle
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

            // Hold the lock only long enough to drain the samples out --
            // the realtime capture callback only ever try_locks this same
            // mutex and drops a period rather than block, so keeping this
            // critical section to a plain drain (not the dB math below)
            // keeps that failure window as short as possible. A poisoned
            // lock (some other panic while it was held) is recovered rather
            // than propagated, so one bad panic doesn't permanently stall
            // metering.
            let mut drained: [Vec<i32>; NUM_CHANNELS] = std::array::from_fn(|_| Vec::new());
            {
                let mut buf = match buffer.lock() {
                    Ok(guard) => guard,
                    Err(err) => err.into_inner(),
                };
                for ch in 0..NUM_CHANNELS {
                    if buf[ch].len() >= FRAMES_PER_UPDATE {
                        // Drain everything currently buffered, not just one
                        // window's worth -- draining only FRAMES_PER_UPDATE
                        // per tick would let a channel that backed up (e.g.
                        // during try_lock contention) permanently plateau at
                        // a higher resident size instead of catching back up
                        // to real time.
                        drained[ch] = buf[ch].drain(..).collect();
                    }
                    // else: not enough data yet -- leave it buffered for
                    // next tick rather than discarding a partial window.
                }
            }

            // Start from the previous levels so a tick with no new data
            // (rare: warm-up, or a dropped period) holds the last known
            // level instead of flashing to a false zero.
            let mut levels = match state_for_proc.lock() {
                Ok(s) => s.channels.clone(),
                Err(err) => err.into_inner().channels.clone(),
            };
            for ch in 0..NUM_CHANNELS {
                let samples = &drained[ch];
                if !samples.is_empty() {
                    let rms_db =
                        decibel::calculate_rms_db(samples, MAX_VALUE_S32, MIN_DB, MAX_DB);
                    let peak_db =
                        decibel::calculate_peak_db(samples, MAX_VALUE_S32, MIN_DB, MAX_DB);
                    let clipping = decibel::detect_clipping(samples, MAX_VALUE_S32);

                    levels[ch] = ChannelLevels {
                        rms_u8: decibel::db_to_u8(rms_db, MIN_DB, MAX_DB),
                        peak_u8: decibel::db_to_u8(peak_db, MIN_DB, MAX_DB),
                        clipping,
                    };
                }
            }

            match state_for_proc.lock() {
                Ok(mut s) => s.channels = levels,
                Err(err) => err.into_inner().channels = levels,
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
    buffer: Arc<Mutex<Vec<VecDeque<i32>>>>,
    dropped_periods: Arc<AtomicUsize>,
    dropped_samples: Arc<AtomicUsize>,
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

                        // This runs on PipeWire's realtime thread. It must
                        // never block on a lock the non-RT processing thread
                        // can hold for a nontrivial time -- that blocking is
                        // what produced the millions of xruns reported in
                        // https://github.com/hifiberry/vu-meter/issues/1.
                        // If the processing thread has it, drop this period
                        // instead of waiting for it. A poisoned lock (the
                        // processing thread panicked while holding it) is
                        // still just data to us, so recover it rather than
                        // treating it the same as contention and going
                        // silent forever.
                        let mut buf = match buffer.try_lock() {
                            Ok(guard) => guard,
                            Err(TryLockError::WouldBlock) => {
                                dropped_periods.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                            Err(TryLockError::Poisoned(err)) => err.into_inner(),
                        };
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
                                    if push_bounded(&mut buf[ch], sample, MAX_BUFFERED_FRAMES) {
                                        dropped_samples.fetch_add(1, Ordering::Relaxed);
                                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_bounded_evicts_oldest_and_stays_bounded() {
        let mut buf = VecDeque::with_capacity(4);
        let mut evictions = 0;
        for sample in 0..1000 {
            if push_bounded(&mut buf, sample, 4) {
                evictions += 1;
            }
        }
        assert_eq!(buf.len(), 4);
        assert_eq!(evictions, 1000 - 4);
        // Keeps the newest samples, not the oldest -- a live meter should
        // reflect what's playing now, not audio from a thousand samples ago.
        assert_eq!(buf, VecDeque::from(vec![996, 997, 998, 999]));
    }

    #[test]
    fn new_channel_buffers_preallocates_to_the_bound() {
        let buffers = new_channel_buffers();
        assert_eq!(buffers.len(), NUM_CHANNELS);
        for buf in &buffers {
            assert!(buf.capacity() >= MAX_BUFFERED_FRAMES);
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn poisoned_buffer_lock_is_recoverable_not_fatal() {
        let buffer: Arc<Mutex<Vec<VecDeque<i32>>>> = Arc::new(Mutex::new(new_channel_buffers()));
        let poisoner = buffer.clone();
        let _ = thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("simulated panic while holding the buffer lock");
        })
        .join();

        // The realtime callback must be able to tell a poisoned lock apart
        // from ordinary contention (WouldBlock) and recover it, rather than
        // treating both the same and dropping capture forever.
        let lock_result = buffer.try_lock();
        match lock_result {
            Err(TryLockError::Poisoned(err)) => {
                drop(err.into_inner());
            }
            Err(TryLockError::WouldBlock) => panic!("expected Poisoned, got WouldBlock"),
            Ok(_) => panic!("expected Poisoned, got Ok"),
        }
    }
}
