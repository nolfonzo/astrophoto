# astrophoto

Remote astrophotography control for Sony a6400 + Sky-Watcher Star Adventurer 2i, running on a Raspberry Pi 4 via Docker.

## Stack

| Service | Purpose | Port |
|---|---|---|
| `indiserver` | INDI server + gphoto2 driver for Sony a6400 | 7624 |
| `indi-webmanager` | Web UI for managing INDI drivers | 8624 |

## Requirements

- Raspberry Pi 4 running Raspberry Pi OS (64-bit)
- Docker + Docker Compose
- Sony a6400 set to **PC Remote** mode (Setup → USB Connection Settings → PC Remote)
- USB-A to Micro-USB cable (Pi → camera)

## Quick Start

```bash
git clone https://github.com/nolfonzo/astrophoto.git
cd astrophoto
docker compose up -d
```

## Remote Control (from laptop)

Install [KStars/Ekos](https://kstars.kde.org) on your laptop, then connect to the INDI server:

- Host: `pi4-black.local` (or Pi's IP)
- Port: `7624`

## Simple Capture (without KStars)

```bash
# SSH into Pi, then:
./scripts/capture.sh [frames] [exposure_secs] [iso] [output_dir]

# Example: 20 frames, 90s each, ISO 3200
./scripts/capture.sh 20 90 3200 /shots
```

## Mount Control (Star Adventurer 2i)

Add `indi_synscan` to the indiserver entrypoint once the SA2i is on the network.
See `config/` for driver configuration.
