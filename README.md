# astrophoto

Remote astrophotography control for Sony a6400 + Sky-Watcher Star Adventurer 2i.

Runs on a Raspberry Pi 4 via Docker. The Pi sits physically close to the camera (USB cable), while a main server (e.g. n150) handles automation via Node-RED and MQTT.

## Architecture

```
Sony a6400 ──USB──► Pi 4 (indiserver + mqtt-bridge)
                         │                    │
                    port 7624            MQTT topics
                         │                    │
                    KStars/Ekos         n150 Node-RED
                    (manual)            (automated)
```

- **Pi 4**: runs `indiserver` (INDI camera driver) and `mqtt-bridge` (MQTT ↔ INDI bridge)
- **Main server**: connects to INDI on port 7624 for manual control via KStars/Ekos, or sends MQTT commands for automated sessions via Node-RED

## Stack

| Service | Purpose | Port |
|---|---|---|
| `indiserver` | INDI server + gphoto2 driver for Sony a6400 | 7624 |
| `mqtt-bridge` | Bridges MQTT commands to INDI capture | — |

## Requirements

- Raspberry Pi 4 running Raspberry Pi OS (64-bit)
- Docker + Docker Compose
- Sony a6400 set to **PC Remote** mode (Setup → USB Connection Settings → PC Remote)
- USB-A to Micro-USB cable (Pi → camera)
- MQTT broker accessible on the network (e.g. Mosquitto on your main server)

## Quick Start

```bash
git clone https://github.com/nolfonzo/astrophoto.git
cd astrophoto
cp .env.example .env
# edit .env: set MQTT_HOST to your broker, TZ to your timezone
docker compose up -d
```

## Remote Control (from laptop)

Install [KStars/Ekos](https://kstars.kde.org) on your laptop, then connect to the INDI server:

- Host: `pi4-black.local` (or Pi's IP)
- Port: `7624`

## Node-RED Automation (MQTT API)

### Trigger a capture session

Publish to `astrophoto/command/capture`:

```json
{
  "frames": 20,
  "exposure": 90,
  "iso": 3200,
  "output": "/shots"
}
```

### Abort

Publish to `astrophoto/command/abort` (empty payload).

### Subscribe to events

| Topic | Payload | Description |
|---|---|---|
| `astrophoto/status` | `{"state": "capturing", "frame": 5, "total": 20}` | Current state |
| `astrophoto/event/frame` | `{"frame": 5, "total": 20}` | Frame captured |
| `astrophoto/event/complete` | `{"frames": 20}` | Session finished |
| `astrophoto/event/aborted` | `{"frame": 3, "total": 20}` | Aborted mid-session |
| `astrophoto/event/error` | `{"message": "..."}` | Error occurred |

## Mount Control (Star Adventurer 2i)

Add `indi_synscan` to the indiserver entrypoint once the SA2i is on the network:

```bash
exec indiserver -v -p 7624 indi_gphoto_ccd indi_synscan
```

See `config/` for driver configuration.
