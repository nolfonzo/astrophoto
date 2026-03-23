#!/usr/bin/env bash
# mqtt-bridge smoke tests — run after redeployment to verify all commands work
# Usage: ./test_mqtt.sh [mqtt_host]
set -euo pipefail

HOST="${1:-127.0.0.1}"
PUB="docker exec astrophoto-mosquitto mosquitto_pub -h $HOST"
PASS=0
FAIL=0

SUB_FILE=$(mktemp)
docker exec astrophoto-mosquitto mosquitto_sub -h "$HOST" -t 'astrophoto/#' -v -W 55 > "$SUB_FILE" 2>&1 &
SUB_PID=$!
sleep 0.8

pub() { $PUB -t "astrophoto/$1" -m "$2"; sleep 1.2; }

check() {
  local desc="$1" pattern="$2"
  if grep -qF "$pattern" "$SUB_FILE"; then
    echo "  PASS  $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $desc  (expected: $pattern)"
    FAIL=$((FAIL + 1))
  fi
}

echo ""
echo "=== astrophoto mqtt-bridge smoke tests ==="
echo ""

echo "--- Queries ---"
pub query/camera '{}'
pub query/profile '{}'
pub query/status '{}'
pub query/presets '{}'

echo "--- Profile set ---"
pub command/profile/set '{"exposure": 30, "iso": 1600, "frames": 10}'
pub query/profile '{}'

echo "--- Defaults ---"
pub command/defaults/set '{"exposure": 5.0, "iso": 800, "frames": 2}'
pub command/defaults/load '{}'
pub query/profile '{}'

echo "--- Presets ---"
pub command/profile/set '{"exposure": 120, "iso": 3200, "frames": 30}'
pub command/preset/save '{"name": "_test_nebula"}'
pub command/profile/set '{"exposure": 0.002, "iso": 100, "frames": 5}'
pub command/preset/save '{"name": "_test_moon"}'
pub query/presets '{}'
pub command/preset/load '{"name": "_test_nebula"}'
pub query/profile '{}'

echo "--- Cleanup ---"
pub command/preset/delete '{"name": "_test_nebula"}'
pub command/preset/delete '{"name": "_test_moon"}'
pub command/defaults/set '{"exposure": 0.01, "iso": 400, "frames": 1}'
pub command/defaults/load '{}'

sleep 1
kill "$SUB_PID" 2>/dev/null; wait "$SUB_PID" 2>/dev/null || true

echo ""
echo "--- Results ---"
check "query/camera → event/camera"       '"camera": "a6400"'
check "event/camera has resolution"       '"resolution":'
check "event/camera has profile field"    '"profile": {'
check "query/profile → event/profile"     'event/profile'
check "query/status → status"             '"state": "idle"'
check "status has profile field"          '"profile": {'
check "query/presets → event/presets"     'event/presets'
check "profile/set exposure"              '"exposure": 30'
check "profile/set iso"                   '"iso": 1600'
check "profile/set frames"                '"frames": 10'
check "defaults/set persists"             '"exposure": 5.0'
check "defaults/load resets profile"      '"source": "defaults"'
check "defaults/load new values"          '"exposure": 5.0, "iso": 800'
check "preset/save nebula"                '"action": "saved", "name": "_test_nebula"'
check "preset/save moon"                  '"action": "saved", "name": "_test_moon"'
check "presets list contains test preset" '"_test_nebula"'
check "preset/load restores values"       '"action": "loaded", "name": "_test_nebula"'
check "profile after preset/load"         '"exposure": 120, "iso": 3200'
check "preset/delete nebula"              '"action": "deleted", "name": "_test_nebula"'
check "preset/delete moon"                '"action": "deleted", "name": "_test_moon"'

echo ""
echo "=== $PASS passed, $FAIL failed ==="
rm -f "$SUB_FILE"
[ "$FAIL" -eq 0 ]
