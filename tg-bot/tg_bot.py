#!/usr/bin/env python3
"""
Telegram bot for Sydney astrophoto stack.
Bridges Telegram commands <-> MQTT (mqtt-bridge on Pi).

Commands:
  /astro capture [frames] [exposure_s] [iso]
  /astro abort
  /astro status
  /astro profile <camera>
  /astro defaults [frames] [exposure_s] [iso]
  /astro preview       -- fetch and send latest preview
  /start               -- prints your chat_id
"""

import json
import logging
import os
import threading
import time

import paho.mqtt.client as mqtt
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOKEN       = os.environ["TELEGRAM_TOKEN"]
CHAT_ID     = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
MQTT_HOST   = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT   = int(os.environ.get("MQTT_PORT", "1883"))
PREFIX      = os.environ.get("MQTT_PREFIX", "astrophoto")
FILESERVER  = os.environ.get("FILESERVER_URL", "http://localhost:8080").rstrip("/")

BASE = f"https://api.telegram.org/bot{TOKEN}"

# ── Telegram helpers ───────────────────────────────────────────────────────────

def tg_send(text, chat_id=None):
    cid = chat_id or CHAT_ID
    if not cid:
        log.warning("tg_send: no chat_id configured, dropping: %s", text[:60])
        return
    try:
        requests.post(f"{BASE}/sendMessage",
                      json={"chat_id": cid, "text": text, "parse_mode": "Markdown"},
                      timeout=10)
    except Exception as e:
        log.error("sendMessage error: %s", e)


def tg_send_photo(url, caption="", chat_id=None):
    cid = chat_id or CHAT_ID
    if not cid:
        return
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        requests.post(f"{BASE}/sendPhoto",
                      data={"chat_id": cid, "caption": caption},
                      files={"photo": ("preview.jpg", r.content, "image/jpeg")},
                      timeout=20)
    except Exception as e:
        log.error("sendPhoto error: %s", e)
        tg_send(f"Preview: {url}", chat_id)


# ── MQTT ──────────────────────────────────────────────────────────────────────

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
_last_preview_url = None


def pub(subtopic, payload=None):
    topic = f"{PREFIX}/{subtopic}"
    data = json.dumps(payload) if payload is not None else ""
    mqttc.publish(topic, data)
    log.info("MQTT pub %s %s", topic, data[:80])


def on_mqtt_message(client, userdata, msg):
    global _last_preview_url
    topic = msg.topic[len(PREFIX) + 1:]
    try:
        payload = json.loads(msg.payload)
    except Exception:
        payload = {}

    if topic == "event/frame":
        tg_send(f"Frame {payload.get('frame')}/{payload.get('total')} captured")

    elif topic == "event/complete":
        tg_send(f"Session complete — {payload.get('frames')} frames captured")

    elif topic == "event/aborted":
        tg_send(f"Aborted at frame {payload.get('frame', '?')}/{payload.get('total', '?')}")

    elif topic == "event/error":
        tg_send(f"Error: {payload.get('message', '?')}")

    elif topic == "event/info":
        tg_send(payload.get("message", ""))

    elif topic == "event/preview":
        path = payload.get("path", "")
        filename = path.split("/")[-1]
        url = f"{FILESERVER}/{filename}"
        _last_preview_url = url
        tg_send_photo(url, caption=filename)

    elif topic == "event/profile":
        lines = [f"{k}: {v}" for k, v in payload.items()]
        tg_send("Profile:\n" + "\n".join(lines))

    elif topic == "event/defaults":
        tg_send(f"Defaults: {json.dumps(payload)}")


def on_connect(client, userdata, flags, reason_code, props):
    client.subscribe(f"{PREFIX}/event/#")
    log.info("MQTT connected rc=%s, subscribed to %s/event/#", reason_code, PREFIX)


mqttc.on_message = on_mqtt_message
mqttc.on_connect = on_connect


# ── Command handlers ──────────────────────────────────────────────────────────

def handle_astro(args, chat_id):
    global CHAT_ID
    # Auto-learn chat_id if not configured
    if not CHAT_ID:
        CHAT_ID = chat_id
        log.info("TELEGRAM_CHAT_ID auto-set to %d (add to .env to persist)", chat_id)

    if not args:
        tg_send(
            "*Astrophoto commands:*\n"
            "/astro capture [frames] [exposure\\_s] [iso]\n"
            "/astro abort\n"
            "/astro status\n"
            "/astro profile <camera>\n"
            "/astro defaults [frames] [exposure\\_s] [iso]\n"
            "/astro preview",
            chat_id,
        )
        return

    cmd = args[0].lower()

    if cmd == "capture":
        params = {}
        try:
            if len(args) >= 2: params["frames"]   = int(args[1])
            if len(args) >= 3: params["exposure"]  = float(args[2])
            if len(args) >= 4: params["iso"]       = int(args[3])
        except ValueError as e:
            tg_send(f"Bad argument: {e}", chat_id)
            return
        pub("command/capture", params or None)
        desc = " ".join(f"{k}={v}" for k, v in params.items()) if params else "defaults"
        tg_send(f"Capture started ({desc})", chat_id)

    elif cmd == "abort":
        pub("command/abort")
        tg_send("Abort sent", chat_id)

    elif cmd == "status":
        result = {}
        ev = threading.Event()

        def _status_cb(client, userdata, msg):
            nonlocal result
            try:
                result = json.loads(msg.payload)
            except Exception:
                pass
            ev.set()

        mqttc.message_callback_add(f"{PREFIX}/status", _status_cb)
        pub("query/status")
        ev.wait(timeout=5)
        mqttc.message_callback_remove(f"{PREFIX}/status")

        if result:
            state  = result.get("state",  "?")
            camera = result.get("camera", "?")
            frame  = result.get("frame")
            total  = result.get("total")
            parts  = [f"State: *{state}*", f"Camera: {camera}"]
            if frame is not None:
                parts.append(f"Frame: {frame}/{total}")
            tg_send("\n".join(parts), chat_id)
        else:
            tg_send("No status response (camera may be offline)", chat_id)

    elif cmd == "profile":
        if len(args) < 2:
            tg_send("Usage: /astro profile <camera>  e.g. a6400", chat_id)
            return
        pub("command/profile", {"camera": args[1]})
        tg_send(f"Profile set to {args[1]}", chat_id)

    elif cmd == "defaults":
        params = {}
        try:
            if len(args) >= 2: params["frames"]   = int(args[1])
            if len(args) >= 3: params["exposure"]  = float(args[2])
            if len(args) >= 4: params["iso"]       = int(args[3])
        except ValueError as e:
            tg_send(f"Bad argument: {e}", chat_id)
            return
        if not params:
            pub("query/defaults")
            tg_send("Requested current defaults", chat_id)
        else:
            pub("command/defaults", params)
            tg_send(f"Defaults updated: {params}", chat_id)

    elif cmd == "preview":
        if _last_preview_url:
            tg_send_photo(_last_preview_url, caption="Latest preview", chat_id=chat_id)
        else:
            tg_send("No preview available yet", chat_id)

    else:
        tg_send(f"Unknown: {cmd}. Try /astro for help.", chat_id)


# ── Telegram long-poll ────────────────────────────────────────────────────────

def poll_telegram():
    offset = 0
    log.info("Telegram polling started")
    while True:
        try:
            r = requests.get(f"{BASE}/getUpdates",
                             params={"offset": offset, "timeout": 30},
                             timeout=35)
            data = r.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg  = update.get("message", {})
                text = msg.get("text", "").strip()
                cid  = msg.get("chat", {}).get("id")
                if not cid:
                    continue
                if text.startswith("/start"):
                    tg_send(f"Chat ID: `{cid}`\nAdd to .env as TELEGRAM\\_CHAT\\_ID={cid}", cid)
                elif text.startswith("/astro"):
                    parts = text.split()
                    handle_astro(parts[1:], cid)
        except Exception as e:
            log.error("Telegram poll error: %s", e)
            time.sleep(5)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not CHAT_ID:
        log.warning("TELEGRAM_CHAT_ID not set — message the bot with /start to get your ID")

    for attempt in range(30):
        try:
            mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
            break
        except Exception as e:
            log.error("MQTT connect failed (%s), retry in 10s", e)
            time.sleep(10)
    else:
        log.critical("Could not connect to MQTT, exiting")
        raise SystemExit(1)

    mqttc.loop_start()
    poll_telegram()
