#!/usr/bin/env python3
"""
Camera capture service — runs inside the indiserver container.

Single point of camera hardware control.  Decides gphoto2 (discrete shutter)
vs INDI Bulb based on exposure duration and the caller's engine preference.

API
---
POST /capture  {"exposure": float, "iso": int, "prefix": str, "output": str,
                "engine": "auto"|"star"|"daytime",
                "sensor": {"max_x":int, "max_y":int, "pixel_um":float, "bits":int}}
             → {"ok": true,  "file": "/shots/frame_xxx_1.fits"}
             → {"ok": false, "error": "..."}

GET  /health → {"ok": true}
"""

import glob
import json
import logging
import os
import queue
import re
import socket
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

INDI_HOST   = 'localhost'
INDI_PORT   = int(os.environ.get('INDI_PORT', '7624'))
CAMERA_DEV  = os.environ.get('CAMERA_DEVICE', 'GPhoto CCD')
SERVICE_PORT = int(os.environ.get('CAPTURE_SERVICE_PORT', '7625'))
AUTO_THRESHOLD = 1.0   # seconds: below → gphoto2 discrete; at/above → INDI Bulb

_DEFAULT_SENSOR = {'max_x': 6000, 'max_y': 4000, 'pixel_um': 3.91, 'bits': 14}

_lock = threading.Lock()   # one capture at a time


# ── gphoto2 discrete shutter ──────────────────────────────────────────────────

def _capture_gphoto2(exposure, iso, prefix, output):
    ss = f'1/{round(1/exposure)}' if exposure < 1 else str(round(exposure))
    os.makedirs(output, exist_ok=True)
    cmd = [
        'gphoto2',
        '--set-config', f'shutterspeed={ss}',
        '--set-config', f'iso={iso}',
        '--capture-image-and-download',
        '--filename', os.path.join(output, f'{prefix}_%n.%C'),
        '--force-overwrite',
    ]
    log.info(f'gphoto2 discrete: shutterspeed={ss} iso={iso}')
    t0 = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError(f'gphoto2 failed: {r.stderr.strip()}')
    log.info(f'gphoto2 done in {time.time()-t0:.1f}s')

    candidates = sorted(glob.glob(os.path.join(output, f'{prefix}_*')),
                        key=os.path.getmtime, reverse=True)
    if not candidates:
        raise RuntimeError('gphoto2 succeeded but no output file found')
    jpegs = [f for f in candidates if f.lower().endswith(('.jpg', '.jpeg'))]
    return jpegs[0] if jpegs else candidates[0]


# ── minimal INDI client for Bulb captures ─────────────────────────────────────

class _IndiCapture:

    def __init__(self):
        self._sock = None
        self._buf  = ''
        self._q    = queue.Queue()
        self._run  = False
        self._iso_map = {}
        self._upload_dir = '/shots'

    def connect(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self._sock = socket.create_connection((INDI_HOST, INDI_PORT), timeout=5)
                self._sock.settimeout(120)
                self._run = True
                threading.Thread(target=self._reader, daemon=True).start()
                self._send('<getProperties version="1.7"/>')
                return
            except OSError as e:
                log.warning(f'INDI connect: {e}, retrying…')
                time.sleep(2)
        raise ConnectionError(f'Could not connect to INDI at {INDI_HOST}:{INDI_PORT}')

    def close(self):
        self._run = False
        try: self._sock.close()
        except Exception: pass

    def _send(self, xml):
        self._sock.sendall(xml.encode())

    def _reader(self):
        while self._run:
            try:
                data = self._sock.recv(65536)
                if not data:
                    break
                self._buf += data.decode('utf-8', errors='replace')
                self._parse()
            except Exception as e:
                if self._run:
                    log.error(f'INDI reader: {e}')
                break

    def _parse(self):
        while True:
            self._buf = self._buf.lstrip()
            if not self._buf:
                return
            if not self._buf.startswith('<'):
                idx = self._buf.find('<')
                if idx < 0: self._buf = ''; return
                self._buf = self._buf[idx:]
            m = re.match(r'<([A-Za-z]\w*)', self._buf)
            if not m: self._buf = self._buf[1:]; continue
            tag = m.group(1)
            gt = self._buf.find('>')
            if gt < 0: return
            if self._buf[gt-1] == '/':
                frag = self._buf[:gt+1]; self._buf = self._buf[gt+1:]
                self._dispatch(tag, frag); continue
            close = f'</{tag}>'
            end = self._buf.find(close)
            if end < 0: return
            frag = self._buf[:end+len(close)]; self._buf = self._buf[end+len(close):]
            if tag == 'setBLOBVector':
                self._q.put({'tag': 'blob'})
            else:
                self._dispatch(tag, frag)

    def _dispatch(self, tag, xml_str):
        try:
            self._q.put({'tag': tag, 'elem': ET.fromstring(xml_str)})
        except ET.ParseError:
            pass

    def _wait_for(self, pred, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                ev = self._q.get(timeout=1)
                if pred(ev): return ev
            except queue.Empty:
                pass
        return None

    def _sw(self, name, **switches):
        inner = ''.join(f'<oneSwitch name="{k}">{v}</oneSwitch>'
                        for k, v in switches.items())
        self._send(f'<newSwitchVector device="{CAMERA_DEV}" name="{name}">{inner}</newSwitchVector>')

    def connect_device(self):
        self._sw('CONNECTION', CONNECT='On', DISCONNECT='Off')

    def disconnect_device(self):
        try:
            self._sw('CONNECTION', CONNECT='Off', DISCONNECT='On')
            time.sleep(1)
        except Exception:
            pass

    def wait_ready(self, timeout=60):
        time.sleep(1)
        self.connect_device()
        deadline = time.time() + timeout
        found = False
        drain_until = None
        while True:
            now = time.time()
            if drain_until:
                if now > drain_until: break
            elif now > deadline:
                break
            try:
                ev = self._q.get(timeout=1)
                e  = ev.get('elem')
                if e is None or e.get('device') != CAMERA_DEV: continue
                tag, name = ev['tag'], e.get('name', '')
                if tag == 'defSwitchVector' and name == 'CCD_ISO':
                    for c in e:
                        if c.get('label', '').strip() and c.get('name', ''):
                            self._iso_map[c.get('label').strip()] = c.get('name')
                elif tag in ('defNumberVector', 'setNumberVector') and name == 'CCD_EXPOSURE':
                    if not found:
                        found = True
                        drain_until = time.time() + 3
            except queue.Empty:
                if drain_until: break
        return found

    def set_upload_local(self, directory, prefix):
        self._upload_dir = directory
        self._sw('UPLOAD_MODE', UPLOAD_LOCAL='On', UPLOAD_CLIENT='Off', UPLOAD_BOTH='Off')
        self._send(
            f'<newTextVector device="{CAMERA_DEV}" name="UPLOAD_SETTINGS">'
            f'<oneText name="UPLOAD_DIR">{directory}</oneText>'
            f'<oneText name="UPLOAD_PREFIX">{prefix}</oneText>'
            f'</newTextVector>'
        )

    def set_ccd_info(self, sensor):
        self._send(
            f'<newNumberVector device="{CAMERA_DEV}" name="CCD_INFO">'
            f'<oneNumber name="CCD_MAX_X">{sensor["max_x"]}</oneNumber>'
            f'<oneNumber name="CCD_MAX_Y">{sensor["max_y"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE">{sensor["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE_X">{sensor["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE_Y">{sensor["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_BITSPERPIXEL">{sensor["bits"]}</oneNumber>'
            f'</newNumberVector>'
        )

    def set_capture_target_ram(self):
        self._sw('CCD_CAPTURE_TARGET', RAM='On', **{'SD Card': 'Off'})

    def set_iso(self, iso):
        switch = self._iso_map.get(str(iso))
        if not switch:
            log.warning(f'ISO {iso} not in map ({list(self._iso_map.keys())}), skipping')
            return
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="CCD_ISO">'
            f'<oneSwitch name="{switch}">On</oneSwitch>'
            f'</newSwitchVector>'
        )

    def expose(self, seconds):
        self._send(
            f'<newNumberVector device="{CAMERA_DEV}" name="CCD_EXPOSURE">'
            f'<oneNumber name="CCD_EXPOSURE_VALUE">{seconds}</oneNumber>'
            f'</newNumberVector>'
        )
        seen = [False]; alerted = [False]

        def done(ev):
            e = ev.get('elem')
            if e is None or ev['tag'] != 'setNumberVector': return False
            if e.get('device') != CAMERA_DEV or e.get('name') != 'CCD_EXPOSURE': return False
            state = e.get('state', '')
            if state == 'Busy':   seen[0] = True; return False
            if state == 'Alert':  alerted[0] = True; return True
            return seen[0] and state == 'Ok'

        self._wait_for(done, timeout=seconds + 120)
        if alerted[0]:
            raise RuntimeError('CCD_EXPOSURE returned Alert')


def _capture_indi_bulb(exposure, iso, prefix, output, sensor):
    os.makedirs(output, exist_ok=True)
    indi = _IndiCapture()
    try:
        indi.connect(timeout=30)
        if not indi.wait_ready(timeout=30):
            raise RuntimeError('Camera driver not ready in INDI server')
        indi.set_upload_local(output, prefix)
        indi.set_ccd_info(sensor)
        indi.set_capture_target_ram()
        time.sleep(1)
        if iso is not None:
            indi.set_iso(iso)
        log.info(f'INDI Bulb: {exposure}s iso={iso}')
        t0 = time.time()
        indi.expose(exposure)
        log.info(f'INDI Bulb done in {time.time()-t0:.1f}s')
    finally:
        indi.disconnect_device()
        indi.close()

    candidates = sorted(
        glob.glob(os.path.join(output, f'{prefix}*')),
        key=os.path.getmtime, reverse=True
    )
    if not candidates:
        raise RuntimeError('INDI capture succeeded but no output file found')
    return candidates[0]


# ── HTTP handler ──────────────────────────────────────────────────────────────

def _do_capture(data):
    exposure = float(data['exposure'])
    iso      = int(data['iso'])
    prefix   = data['prefix']
    output   = data.get('output', '/shots')
    engine   = data.get('engine', 'auto')
    sensor   = {**_DEFAULT_SENSOR, **data.get('sensor', {})}

    if engine == 'daytime' or (engine == 'auto' and exposure < AUTO_THRESHOLD):
        return _capture_gphoto2(exposure, iso, prefix, output)
    else:
        return _capture_indi_bulb(exposure, iso, prefix, output, sensor)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.debug(f'HTTP {fmt % args}')

    def do_GET(self):
        if self.path == '/health':
            self._respond(200, {'ok': True})
        else:
            self._respond(404, {'ok': False, 'error': 'not found'})

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        try:
            data = json.loads(self.rfile.read(length))
        except Exception:
            self._respond(400, {'ok': False, 'error': 'bad JSON'})
            return

        if self.path != '/capture':
            self._respond(404, {'ok': False, 'error': 'not found'})
            return

        with _lock:
            try:
                file_path = _do_capture(data)
                self._respond(200, {'ok': True, 'file': file_path})
            except Exception as e:
                log.error(f'Capture error: {e}')
                self._respond(200, {'ok': False, 'error': str(e)})

    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    server = HTTPServer(('', SERVICE_PORT), _Handler)
    log.info(f'Capture service listening on :{SERVICE_PORT}')
    server.serve_forever()
