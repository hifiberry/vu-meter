# HiFiBerry VU Meter Service

Real-time audio level monitoring for HiFiBerry OS. Captures audio from PipeWire and streams levels to the web UI via WebSocket.

## How it works

The service connects to the PipeWire audio graph, captures PCM samples from the default audio sink's monitor, and computes RMS/peak levels for each channel. Levels are streamed to connected WebSocket clients as compact 6-byte binary frames at 10 Hz.

## WebSocket API

**Endpoint:** `ws://localhost:2717/api/v1/levels`

Each frame is 6 bytes:

| Byte | Description |
|------|-------------|
| 0 | Left channel RMS (0–255, maps -60 dB to 0 dB) |
| 1 | Left channel peak (0–255) |
| 2 | Right channel RMS (0–255) |
| 3 | Right channel peak (0–255) |
| 4 | Flags (bit 0: left clipping, bit 1: right clipping) |
| 5 | Number of channels |

Silence produces all zeros. No subscription message needed — connect and start receiving.

## REST API

- `GET /api/v1/version` — returns `{ "version": "...", "api_version": "1.0" }`
- `GET /version` — same as above

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VU_METER_PORT` | `2717` | HTTP/WebSocket listen port |
| `VU_METER_TARGET` | *(auto)* | PipeWire node name to monitor (auto-detects default sink if unset) |

## Building

```bash
cargo build --release
```

Requires `libpipewire-0.3-dev` and `libclang-dev`.

## Debian package

```bash
# From the packages/vu-meter/ directory in hifiberry-os:
./build.sh
```

Installs:
- `/usr/bin/vu-meter-service` — the binary
- `/usr/lib/systemd/user/vu-meter.service` — systemd user service (runs after PipeWire)
- Nginx proxy config for `/api/vu-meter/` → `localhost:2717`

## Running

The service runs as a systemd user service alongside PipeWire:

```bash
systemctl --user enable vu-meter
systemctl --user start vu-meter
```

## Testing

Quick test with Python:

```python
import asyncio, websockets

async def main():
    async with websockets.connect("ws://localhost:2717/api/v1/levels") as ws:
        async for msg in ws:
            data = list(msg)
            print(f"L: rms={data[0]} peak={data[1]}  R: rms={data[2]} peak={data[3]}  flags={data[4]:02b}  ch={data[5]}")

asyncio.run(main())
```

## License

MIT
