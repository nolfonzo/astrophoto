#!/usr/bin/env python3
"""
MQTT bridge for astrophoto INDI server.

Subscribes to MQTT commands and controls the camera via the INDI protocol.

Command topics  (n150 → Pi 4):
  astrophoto/command/capture  {"frames": 20, "exposure": 90, "iso": 3200, "output": "/shots"}
  astrophoto/command/abort

Status topics  (Pi 4 → n150):
  astrophoto/status           {"state": "idle|capturing|error", "frame": N, "total": N}
  astrophoto/event/frame      {"frame": N, "total": N}
  astrophoto/event/complete   {"frames": N}
  astrophoto/event/aborted    {"frame": N, "total": N}
  astrophoto/event/error      {"message": "..."}
"""

import json
import logging
import os
import queue
import re
import socket
import threading
import time
import xml.etree.ElementTree as ET

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

MQTT_HOST    = os.environ.get('MQTT_HOST', 'localhost')
MQTT_PORT    = int(os.environ.get('MQTT_PORT', '1883'))
MQTT_PREFIX  = os.environ.get('MQTT_PREFIX', 'astrophoto')
INDI_HOST    = os.environ.get('INDI_HOST', 'localhost')
INDI_PORT    = int(os.environ.get('INDI_PORT', '7624'))
CAMERA_DEV   = os.environ.get('CAMERA_DEVICE', 'GPhoto CCD')


class INDIClient:
    """Lightweight INDI protocol client over TCP."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None
        self._buf = ''
        self._events = queue.Queue()
        self._running = False

    def connect(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self._sock = socket.create_connection((self.host, self.port), timeout=5)
                self._running = True
                threading.Thread(target=self._reader, daemon=True).start()
                self._send('<getProperties version="1.7"/>')
                log.info(f'INDI connected at {self.host}:{self.port}')
                return
            except OSError as e:
                log.warning(f'INDI connect failed ({e}), retrying...')
                time.sleep(2)
        raise ConnectionError(f'Could not connect to INDI at {self.host}:{self.port}')

    def close(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass

    def _send(self, xml: str):
        self._sock.sendall(xml.encode())

    def _reader(self):
        while self._running:
            try:
                data = self._sock.recv(65536)
                if not data:
                    break
                self._buf += data.decode('utf-8', errors='replace')
                self._parse()
            except Exception as e:
                if self._running:
                    log.error(f'INDI reader: {e}')
                break

    def _parse(self):
        """Extract complete XML elements from the buffer."""
        while True:
            self._buf = self._buf.lstrip()
            if not self._buf:
                return
            if not self._buf.startswith('<'):
                idx = self._buf.find('<')
                if idx < 0:
                    self._buf = ''
                    return
                self._buf = self._buf[idx:]

            m = re.match(r'<([A-Za-z]\w*)', self._buf)
            if not m:
                self._buf = self._buf[1:]
                continue

            tag = m.group(1)
            gt = self._buf.find('>')
            if gt < 0:
                return  # Incomplete opening tag, wait

            if self._buf[gt - 1] == '/':
                # Self-closing element
                frag = self._buf[:gt + 1]
                self._buf = self._buf[gt + 1:]
                self._dispatch(tag, frag)
                continue

            close = f'</{tag}>'
            end = self._buf.find(close)
            if end < 0:
                return  # Incomplete element, wait for more data

            frag = self._buf[:end + len(close)]
            self._buf = self._buf[end + len(close):]

            if tag == 'setBLOBVector':
                # BLOBs are not needed (we use LOCAL upload mode)
                self._events.put({'tag': 'blob'})
            else:
                self._dispatch(tag, frag)

    def _dispatch(self, tag, xml_str):
        try:
            elem = ET.fromstring(xml_str)
            self._events.put({'tag': tag, 'elem': elem})
        except ET.ParseError:
            pass

    def wait_for(self, predicate, timeout=300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                event = self._events.get(timeout=1)
                if predicate(event):
                    return event
            except queue.Empty:
                pass
        return None

    def wait_ready(self, timeout=30):
        """Wait until the camera driver exposes CCD_EXPOSURE."""
        def has_exposure(ev):
            e = ev.get('elem')
            return (e is not None
                    and ev['tag'] in ('defNumberVector', 'setNumberVector')
                    and e.get('device') == CAMERA_DEV
                    and e.get('name') == 'CCD_EXPOSURE')
        return self.wait_for(has_exposure, timeout=timeout) is not None

    # ---- INDI commands ----

    def set_upload_local(self, directory):
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="UPLOAD_MODE">'
            f'<oneSwitch name="UPLOAD_LOCAL">On</oneSwitch>'
            f'<oneSwitch name="UPLOAD_CLIENT">Off</oneSwitch>'
            f'<oneSwitch name="UPLOAD_BOTH">Off</oneSwitch>'
            f'</newSwitchVector>'
        )
        self._send(
            f'<newTextVector device="{CAMERA_DEV}" name="UPLOAD_SETTINGS">'
            f'<oneText name="UPLOAD_DIR">{directory}</oneText>'
            f'<oneText name="UPLOAD_PREFIX">frame_</oneText>'
            f'</newTextVector>'
        )

    def set_iso(self, iso):
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="CCD_ISO">'
            f'<oneSwitch name="ISO_{iso}">On</oneSwitch>'
            f'</newSwitchVector>'
        )

    def expose(self, seconds):
        self._send(
            f'<newNumberVector device="{CAMERA_DEV}" name="CCD_EXPOSURE">'
            f'<oneNumber name="CCD_EXPOSURE_VALUE">{seconds}</oneNumber>'
            f'</newNumberVector>'
        )

        def exposure_done(ev):
            e = ev.get('elem')
            if e is None or ev['tag'] != 'setNumberVector':
                return False
            if e.get('device') != CAMERA_DEV or e.get('name') != 'CCD_EXPOSURE':
                return False
            for child in e:
                if child.get('name') == 'CCD_EXPOSURE_VALUE':
                    try:
                        return float(child.text.strip()) == 0.0
                    except (ValueError, AttributeError):
                        pass
            return False

        return self.wait_for(exposure_done, timeout=seconds + 60) is not None


# ---- Capture logic ----

_capturing = False
_capture_lock = threading.Lock()
_mqtt_client = None


def pub(subtopic, payload):
    if _mqtt_client:
        _mqtt_client.publish(f'{MQTT_PREFIX}/{subtopic}', json.dumps(payload))


def run_capture(params):
    global _capturing

    frames   = int(params.get('frames', 10))
    exposure = float(params.get('exposure', 60))
    iso      = int(params.get('iso', 1600))
    output   = params.get('output', '/shots')

    log.info(f'Capture: {frames}x {exposure}s ISO{iso} -> {output}')
    pub('status', {'state': 'capturing', 'frame': 0, 'total': frames,
                   'exposure': exposure, 'iso': iso})

    indi = INDIClient(INDI_HOST, INDI_PORT)
    try:
        indi.connect(timeout=30)
        if not indi.wait_ready(timeout=30):
            raise RuntimeError('Camera driver not ready in INDI server')
        os.makedirs(output, exist_ok=True)
        indi.set_upload_local(output)
    except Exception as e:
        log.error(f'INDI setup failed: {e}')
        pub('event/error', {'message': str(e)})
        pub('status', {'state': 'idle'})
        with _capture_lock:
            _capturing = False
        return

    try:
        for i in range(1, frames + 1):
            with _capture_lock:
                if not _capturing:
                    pub('event/aborted', {'frame': i - 1, 'total': frames})
                    pub('status', {'state': 'idle'})
                    return

            log.info(f'Frame {i}/{frames}')
            pub('status', {'state': 'capturing', 'frame': i, 'total': frames})

            indi.set_iso(iso)
            if not indi.expose(exposure):
                pub('event/error', {'message': f'Exposure timed out at frame {i}'})
                pub('status', {'state': 'idle'})
                return

            pub('event/frame', {'frame': i, 'total': frames})

        pub('event/complete', {'frames': frames})
        pub('status', {'state': 'idle'})
        log.info('Session complete')
    finally:
        indi.close()
        with _capture_lock:
            _capturing = False


# ---- MQTT callbacks ----

def on_message(client, userdata, msg):
    global _capturing
    topic = msg.topic[len(MQTT_PREFIX) + 1:]
    try:
        payload = json.loads(msg.payload) if msg.payload else {}
    except Exception:
        payload = {}

    log.info(f'<- {topic}: {payload}')

    if topic == 'command/capture':
        with _capture_lock:
            if _capturing:
                pub('event/error', {'message': 'Already capturing'})
                return
            _capturing = True
        threading.Thread(target=run_capture, args=(payload,), daemon=True).start()

    elif topic == 'command/abort':
        with _capture_lock:
            _capturing = False
        log.info('Abort signalled')


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info(f'MQTT connected to {MQTT_HOST}:{MQTT_PORT}')
        client.subscribe(f'{MQTT_PREFIX}/command/#')
        pub('status', {'state': 'idle'})
    else:
        log.error(f'MQTT connect failed rc={rc}')


def main():
    global _mqtt_client
    _mqtt_client = mqtt.Client()
    _mqtt_client.on_connect = on_connect
    _mqtt_client.on_message = on_message

    while True:
        try:
            _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            _mqtt_client.loop_forever()
        except Exception as e:
            log.error(f'MQTT error: {e}, retry in 5s')
            time.sleep(5)


if __name__ == '__main__':
    main()
