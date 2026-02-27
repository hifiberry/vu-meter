mod capture;
mod decibel;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use capture::MeterState;
use serde::Serialize;
use std::sync::{atomic::Ordering, Arc, Mutex};
use std::time::Duration;
use tokio::time::interval;

#[derive(Clone)]
struct AppState {
    meter: Arc<Mutex<MeterState>>,
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("VU_METER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(2717);

    let target = std::env::var("VU_METER_TARGET").ok();

    eprintln!("Starting VU meter service on port {}", port);
    if let Some(ref t) = target {
        eprintln!("Target: {}", t);
    }

    // Start PipeWire capture in background threads
    let (meter_state, quit_flag) = capture::start_capture(target);

    let state = AppState {
        meter: meter_state,
    };

    let app = Router::new()
        .route("/api/v1/levels", get(ws_handler))
        .route("/api/v1/version", get(get_version))
        .route("/version", get(get_version))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind");

    eprintln!("Listening on 0.0.0.0:{}", port);

    // Graceful shutdown on SIGTERM/SIGINT
    let quit_for_shutdown = quit_flag.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        quit_for_shutdown.store(true, Ordering::Relaxed);
    });

    axum::serve(listener, app).await.expect("Server failed");

    quit_flag.store(true, Ordering::Relaxed);
}

#[derive(Serialize)]
struct VersionResponse {
    version: &'static str,
    api_version: &'static str,
}

async fn get_version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION"),
        api_version: "1.0",
    })
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    // Send binary frames at ~10 Hz
    let mut tick = interval(Duration::from_millis(100));

    loop {
        tick.tick().await;

        let frame = {
            let meter = match state.meter.lock() {
                Ok(m) => m,
                Err(_) => break,
            };

            let num_channels = meter.channels.len();
            // 6-byte frame: L_rms, L_peak, R_rms, R_peak, flags, num_channels
            let mut buf = [0u8; 6];

            if num_channels >= 1 {
                buf[0] = meter.channels[0].rms_u8;
                buf[1] = meter.channels[0].peak_u8;
            }
            if num_channels >= 2 {
                buf[2] = meter.channels[1].rms_u8;
                buf[3] = meter.channels[1].peak_u8;
            }

            let mut flags: u8 = 0;
            if num_channels >= 1 && meter.channels[0].clipping {
                flags |= 0x01;
            }
            if num_channels >= 2 && meter.channels[1].clipping {
                flags |= 0x02;
            }
            buf[4] = flags;
            buf[5] = num_channels as u8;

            buf
        };

        if socket.send(Message::Binary(frame.to_vec().into())).await.is_err() {
            break; // Client disconnected
        }
    }
}
