#!/usr/bin/env bash
# setup.sh — bootstrap astrophoto on a fresh Raspberry Pi
# Usage: bash setup.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
USER_ID="$(id -u)"
GROUP_ID="$(id -g)"

info()  { echo "[setup] $*"; }
ok()    { echo "[setup] ✓ $*"; }
die()   { echo "[setup] ERROR: $*" >&2; exit 1; }

cd "$REPO_DIR"

# ── 1. Docker ─────────────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  info "Installing Docker..."
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "$USER"
  ok "Docker installed. NOTE: log out and back in for group membership to take effect."
  info "Re-running setup via sudo docker to continue..."
  DOCKER="sudo docker"
else
  ok "Docker already installed."
  DOCKER="docker"
fi

if ! $DOCKER compose version &>/dev/null; then
  info "Installing Docker Compose plugin..."
  sudo apt-get install -y docker-compose-plugin
fi
ok "Docker Compose ready."

# ── 2. Hostname
# ─────────────────────────────────────────────────────────────────────────────
if [ "$(hostname)" = "raspberrypi" ] || [ "$(hostname)" = "localhost" ]; then
  read -rp "  Pi hostname (e.g. pi4-black for Chile, pi4-astro-oz for OZ) [raspberrypi]: " HN_INPUT
  HN_VAL="${HN_INPUT:-raspberrypi}"
  if [ "$HN_VAL" != "$(hostname)" ]; then
    sudo hostnamectl set-hostname "$HN_VAL"
    ok "Hostname set to $HN_VAL (takes effect after reboot)"
  fi
fi

# ── 2. .env ───────────────────────────────────────────────────────────────────
if [ ! -f .env ]; then
  info "Creating .env from .env.example..."
  cp .env.example .env

  read -rp "  Timezone [Australia/Sydney]: " TZ_INPUT
  TZ_VAL="${TZ_INPUT:-Australia/Sydney}"
  sed -i "s|TZ=.*|TZ=${TZ_VAL}|" .env

  read -rp "  Max raw files to keep on Pi [200]: " KEEP_INPUT
  KEEP_VAL="${KEEP_INPUT:-200}"
  sed -i "s|SHOTS_KEEP=.*|SHOTS_KEEP=${KEEP_VAL}|" .env

  ok ".env created."
else
  ok ".env already exists, skipping."
fi

# ── 3. Directories ────────────────────────────────────────────────────────────
info "Creating required directories..."
mkdir -p shots config
ok "Directories ready."

# ── 4. Config files (profile + presets) ──────────────────────────────────────
if [ ! -f config/profile.json ]; then
  info "Creating default config/profile.json..."
  cat > config/profile.json << 'JSON'
{
  "camera": "a6400",
  "camera_defaults": {
    "a6400": {
      "frames": 1,
      "exposure": 0.01,
      "iso": 400
    }
  }
}
JSON
  ok "config/profile.json created."
else
  ok "config/profile.json already exists, skipping."
fi

if [ ! -f config/presets.json ]; then
  info "Creating empty config/presets.json..."
  echo '{}' > config/presets.json
  ok "config/presets.json created."
else
  ok "config/presets.json already exists, skipping."
fi

# ── 5. Ownership ──────────────────────────────────────────────────────────────
info "Setting ownership on config/ and shots/..."
sudo chown -R "${USER_ID}:${GROUP_ID}" config/ shots/
ok "Ownership set to ${USER_ID}:${GROUP_ID}."

# ── 6. Build and start ────────────────────────────────────────────────────────
info "Building Docker images..."
$DOCKER compose build

info "Starting containers..."
$DOCKER compose up -d

info "Waiting for mqtt-bridge to connect..."
sleep 6

ok "Containers started:"
$DOCKER compose ps

# ── 7. Smoke tests ────────────────────────────────────────────────────────────
if [ -f test_mqtt.sh ]; then
  info "Running smoke tests..."
  bash test_mqtt.sh && ok "All tests passed." || echo "[setup] WARNING: some tests failed — check logs with: docker logs astrophoto-mqtt"
else
  info "No test_mqtt.sh found, skipping tests."
fi

echo ""
echo "=== Setup complete ==="
echo "  Camera shots:   $REPO_DIR/shots/"
echo "  Config/presets: $REPO_DIR/config/"
echo "  Logs:           docker logs astrophoto-mqtt"
echo "  Test suite:     bash test_mqtt.sh"
