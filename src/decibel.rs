/// Calculate RMS (Root Mean Square) value from audio samples
pub fn calculate_rms(samples: &[i32]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum_squares: f64 = samples.iter().map(|&s| (s as f64).powi(2)).sum();
    (sum_squares / samples.len() as f64).sqrt()
}

/// Calculate peak (maximum absolute) value from audio samples
pub fn calculate_peak(samples: &[i32]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().map(|&s| s.abs()).max().unwrap_or(0) as f64
}

/// Convert RMS value to decibels relative to a reference value
pub fn rms_to_db(rms: f64, reference: f64, min_db: f64) -> f64 {
    if rms < 1.0 {
        return min_db;
    }
    let db = 20.0 * (rms / reference).log10();
    db.max(min_db)
}

/// Calculate RMS in decibels from audio samples
pub fn calculate_rms_db(samples: &[i32], reference: f64, min_db: f64, max_db: f64) -> f64 {
    let rms = calculate_rms(samples);
    rms_to_db(rms, reference, min_db).max(min_db).min(max_db)
}

/// Calculate peak in decibels from audio samples
pub fn calculate_peak_db(samples: &[i32], reference: f64, min_db: f64, max_db: f64) -> f64 {
    let peak = calculate_peak(samples);
    if peak < 1.0 {
        return min_db;
    }
    let db = 20.0 * (peak / reference).log10();
    db.max(min_db).min(max_db)
}

/// Detect if any samples exceed a clipping threshold
pub fn detect_clipping(samples: &[i32], reference: f64) -> bool {
    let threshold = (reference * 0.999) as i32;
    samples.iter().any(|&s| s.abs() >= threshold)
}

/// Map a dB value to a u8 (0-255) for the binary WebSocket protocol.
/// Maps min_db → 0, max_db → 255.
pub fn db_to_u8(db: f64, min_db: f64, max_db: f64) -> u8 {
    let range = max_db - min_db;
    if range <= 0.0 {
        return 0;
    }
    let normalized = ((db - min_db) / range).clamp(0.0, 1.0);
    (normalized * 255.0) as u8
}
