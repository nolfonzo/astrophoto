#!/usr/bin/env python3
"""
MQTT bridge for astrophoto INDI server.

Subscribes to MQTT commands and controls the camera via the INDI protocol.

Command topics  (n150 → Pi 4):
  astrophoto/command/capture   {"frames": 5, "exposure": 90, "iso": 3200}
  astrophoto/command/abort
  astrophoto/command/profile   {"camera": "a6400"}
  astrophoto/command/defaults  {"frames": 1, "exposure": 0.01, "iso": 400}
  astrophoto/query/status

Status topics  (Pi 4 → n150):
  astrophoto/status            {"state": "idle|capturing", "camera": "a6400", "mode": "Manual", ...}
  astrophoto/event/frame       {"frame": N, "total": N}
  astrophoto/event/complete    {"frames": N}
  astrophoto/event/aborted     {"frame": N, "total": N}
  astrophoto/event/error       {"message": "..."}
  astrophoto/event/info        {"message": "..."}   (e.g. ignored params notice)
  astrophoto/event/profile     {"camera": "a6400", ...}
  astrophoto/event/defaults    {"frames": 1, "exposure": 0.01, "iso": 400}
  astrophoto/event/preview     {"path": "/shots/xxx.jpg"}
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

MQTT_HOST   = os.environ.get('MQTT_HOST', 'localhost')
MQTT_PORT   = int(os.environ.get('MQTT_PORT', '1883'))
MQTT_PREFIX = os.environ.get('MQTT_PREFIX', 'astrophoto')
INDI_HOST   = os.environ.get('INDI_HOST', 'localhost')
INDI_PORT   = int(os.environ.get('INDI_PORT', '7624'))
CAMERA_DEV  = os.environ.get('CAMERA_DEVICE', 'GPhoto CCD')
PROFILE_FILE = os.environ.get('PROFILE_FILE', '/config/profile.json')
GPHOTO2_AUTO_THRESHOLD = 1.0  # seconds: below this use gphoto2 discrete, at/above use INDI Bulb

# ---- Camera profiles ----

CAMERA_PROFILES = {
    # min/max_exposure: 1/32000s (electronic shutter) to 30s (PTP limit)
    # min/max_iso: standard range; extended (50, 51200+) requires camera menu
    'a6400': {'max_x': 6000, 'max_y': 4000, 'pixel_um': 3.91, 'bits': 14,
              'min_exposure': 1/32000, 'max_exposure': 30,
              'min_iso': 100, 'max_iso': 32000},
    'a6700': {'max_x': 6192, 'max_y': 4128, 'pixel_um': 3.76, 'bits': 14,
              'min_exposure': 1/32000, 'max_exposure': 30,
              'min_iso': 100, 'max_iso': 32000},
}

_PROFILE_DEFAULTS = {
    'camera': 'a6400',
    'defaults': {'frames': 1, 'exposure': 0.01, 'iso': 400},
}

_profile = {}


def load_profile():
    global _profile
    try:
        with open(PROFILE_FILE) as f:
            _profile = json.load(f)
        log.info(f'Profile loaded: camera={_profile.get("camera")} defaults={_profile.get("defaults")}')
    except FileNotFoundError:
        _profile = {
            'camera': _PROFILE_DEFAULTS['camera'],
            'defaults': dict(_PROFILE_DEFAULTS['defaults']),
        }
        log.info('No profile file found, using defaults')
    except Exception as e:
        log.error(f'Profile load error: {e}, using defaults')
        _profile = {
            'camera': _PROFILE_DEFAULTS['camera'],
            'defaults': dict(_PROFILE_DEFAULTS['defaults']),
        }


def save_profile():
    try:
        os.makedirs(os.path.dirname(PROFILE_FILE), exist_ok=True)
        with open(PROFILE_FILE, 'w') as f:
            json.dump(_profile, f, indent=2)
    except Exception as e:
        log.error(f'Profile save error: {e}')


def current_camera_cfg():
    return CAMERA_PROFILES.get(_profile.get('camera', 'a6400'), CAMERA_PROFILES['a6400'])


def current_defaults():
    return _profile.get('defaults', dict(_PROFILE_DEFAULTS['defaults']))


def current_capture_engine(exposure=None):
    """Return 'star' or 'daytime' based on profile setting and optional exposure.
    'auto' (default): daytime for exposures < GPHOTO2_AUTO_THRESHOLD, star otherwise.
    'star': always use INDI Bulb (long exposures, star tracker).
    'daytime': always use gphoto2 discrete shutter speeds.
    """
    engine = _profile.get('capture_engine', 'auto')
    if engine == 'auto':
        if exposure is not None and exposure < GPHOTO2_AUTO_THRESHOLD:
            return 'daytime'
        return 'star'
    return engine  # 'star' or 'daytime'


# ---- Mode helpers ----

def normalise_mode(mode):
    """Return a short display label for the camera exposure program."""
    m = (mode or '').lower().strip()
    if 'manual' in m or m == 'm':    return 'Manual'
    if 'aperture' in m or m == 'a':  return 'Aperture'
    if 'shutter' in m or m == 's':   return 'Shutter'
    if 'program' in m or m == 'p':   return 'Program'
    if 'auto' in m:                  return 'Auto'
    if 'intelligent' in m:           return 'Auto'
    return mode.strip() if mode else 'Unknown'


def is_manual(mode):
    m = (mode or '').lower().strip()
    return 'manual' in m or m == 'm'


def resolve_capture_params(params, mode):
    """
    Apply mode-aware param filtering.
    Returns (frames, exposure, iso, ignored_keys).
    iso=None means skip ISO setting (Auto ISO mode).
    """
    defaults = current_defaults()
    frames = int(params.get('frames', defaults['frames']))
    ignored = []

    cfg = current_camera_cfg()
    if is_manual(mode):
        exposure = float(params.get('exposure', defaults['exposure']))
        min_exp = cfg['min_exposure']
        max_exp = cfg['max_exposure']
        if exposure < min_exp:
            log.warning(f'Exposure {exposure}s below camera minimum; clamped to {min_exp}s')
            ignored.append(f'exposure clamped to {min_exp}s (below camera minimum)')
            exposure = min_exp
        elif exposure > max_exp:
            log.warning(f'Exposure {exposure}s above camera maximum; clamped to {max_exp}s')
            ignored.append(f'exposure clamped to {max_exp}s (above camera maximum)')
            exposure = max_exp
        iso = int(params.get('iso', defaults['iso']))
        iso = max(cfg['min_iso'], min(cfg['max_iso'], iso))
    elif normalise_mode(mode) in ('Aperture', 'Shutter', 'Program'):
        # Camera controls shutter or both; just trigger with minimum value
        exposure = 0.001
        iso = int(params.get('iso', defaults['iso']))
        iso = max(cfg['min_iso'], min(cfg['max_iso'], iso))
        if 'exposure' in params:
            ignored.append('exposure')
    else:
        # Auto / unknown — camera controls everything, don't override ISO either
        exposure = 0.001
        iso = None
        if 'exposure' in params:
            ignored.append('exposure')
        if 'iso' in params:
            ignored.append('iso')

    return frames, exposure, iso, ignored


# ---- File discovery ----

def find_latest_frame(output_dir, after_time):
    """Find the most recently modified FITS/JPG/RAW file created after after_time."""
    import glob as _glob
    candidates = []
    for pattern in ('*.fits', '*.fit', '*.jpg', '*.jpeg', '*.arw', '*.ARW'):
        candidates.extend(_glob.glob(os.path.join(output_dir, pattern)))
    recent = [f for f in candidates if os.path.getmtime(f) >= after_time]
    if not recent:
        return None
    return max(recent, key=os.path.getmtime)


# ---- JPEG preview extraction ----

def _parse_fits_header(data):
    """Return (header_bytes, {keyword: value}) from raw FITS bytes."""
    kv = {}
    block_num = 0
    while True:
        block = data[block_num * 2880:(block_num + 1) * 2880]
        if not block:
            break
        found_end = False
        for i in range(0, 2880, 80):
            rec = block[i:i + 80].decode('ascii', errors='replace')
            key = rec[:8].strip()
            if key == 'END':
                found_end = True
                break
            if '=' in rec[:30]:
                k = rec[:8].strip()
                val_str = rec[10:].split('/')[0].strip().strip("'").strip()
                try:
                    kv[k] = int(val_str)
                except ValueError:
                    try:
                        kv[k] = float(val_str)
                    except ValueError:
                        kv[k] = val_str
        block_num += 1
        if found_end:
            break
    return block_num * 2880, kv


def extract_jpeg_previews(fits_path, preview_max_dim=1920):
    """
    Convert a FITS file into two JPEGs in one pass:
      - _preview.jpg : downscaled (≤1920px), quality 85  — for Telegram
      - _hq.jpg      : full resolution,      quality 92  — for archive
    Returns (preview_path, hq_path); either may be None on failure.
    """
    try:
        from PIL import Image
    except ImportError:
        log.warning('Pillow not available — cannot generate preview')
        return None, None
    try:
        with open(fits_path, 'rb') as f:
            raw = f.read()

        header_bytes, hdr = _parse_fits_header(raw)
        bitpix = hdr.get('BITPIX', 8)
        naxis1 = hdr.get('NAXIS1', 0)
        naxis2 = hdr.get('NAXIS2', 0)
        naxis3 = hdr.get('NAXIS3', 1)

        if naxis1 <= 0 or naxis2 <= 0:
            log.warning(f'FITS header parse failed: {hdr}')
            return None, None

        pixel_data = raw[header_bytes:]
        bytes_per_pixel = abs(bitpix) // 8
        plane_size = naxis1 * naxis2 * bytes_per_pixel
        w, h = naxis1, naxis2

        if naxis3 == 3 and len(pixel_data) >= 3 * plane_size:
            if bitpix == 8:
                r = Image.frombytes('L', (w, h), pixel_data[:plane_size])
                g = Image.frombytes('L', (w, h), pixel_data[plane_size:2 * plane_size])
                b = Image.frombytes('L', (w, h), pixel_data[2 * plane_size:3 * plane_size])
                img = Image.merge('RGB', (r, g, b))
            elif bitpix == 16:
                import struct as _struct
                arr = _struct.unpack(f'>{naxis1 * naxis2 * 3}H', pixel_data[:3 * plane_size])
                scale = 255.0 / 65535.0
                pixels_8 = bytes(int(v * scale) for v in arr)
                r = Image.frombytes('L', (w, h), pixels_8[:naxis1 * naxis2])
                g = Image.frombytes('L', (w, h), pixels_8[naxis1 * naxis2:2 * naxis1 * naxis2])
                b = Image.frombytes('L', (w, h), pixels_8[2 * naxis1 * naxis2:])
                img = Image.merge('RGB', (r, g, b))
            else:
                log.warning(f'Unsupported BITPIX={bitpix}')
                return None, None
        elif naxis3 == 1 or naxis3 == 0:
            if bitpix == 8:
                img = Image.frombytes('L', (w, h), pixel_data[:plane_size])
            else:
                log.warning(f'Unsupported grayscale BITPIX={bitpix}')
                return None, None
        else:
            log.warning(f'Unexpected NAXIS3={naxis3}')
            return None, None

        base = os.path.splitext(fits_path)[0]

        # HQ JPEG — full resolution for archive
        hq_path = base + '_hq.jpg'
        img.save(hq_path, 'JPEG', quality=92)
        log.info(f'HQ JPEG: {hq_path} ({w}×{h})')

        # Preview JPEG — downscaled for Telegram
        if max(w, h) > preview_max_dim:
            scale = preview_max_dim / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        preview_path = base + '_preview.jpg'
        img.save(preview_path, 'JPEG', quality=85)
        log.info(f'Preview JPEG: {preview_path} ({img.size[0]}×{img.size[1]})')

        return preview_path, hq_path
    except Exception as e:
        log.warning(f'JPEG generation from {fits_path}: {e}')
        return None, None


# Keep old name as alias for non-FITS callers
def extract_jpeg_preview(fits_path):
    preview, _ = extract_jpeg_previews(fits_path)
    return preview


# ---- INDI client ----

class INDIClient:
    """Lightweight INDI protocol client over TCP."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None
        self._buf = ''
        self._events = queue.Queue()
        self._running = False
        self._iso_map = {}       # label (e.g. "800") -> switch name (e.g. "ISO14")
        self._expprogram = None  # current exposure program label
        self._battery = None     # battery level string (e.g. "68%")
        self._upload_dir = '/shots'

    def connect(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self._sock = socket.create_connection((self.host, self.port), timeout=5)
                self._sock.settimeout(120)
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
                return

            if self._buf[gt - 1] == '/':
                frag = self._buf[:gt + 1]
                self._buf = self._buf[gt + 1:]
                self._dispatch(tag, frag)
                continue

            close = f'</{tag}>'
            end = self._buf.find(close)
            if end < 0:
                return

            frag = self._buf[:end + len(close)]
            self._buf = self._buf[end + len(close):]

            if tag == 'setBLOBVector':
                self._events.put({'tag': 'blob'})
            else:
                self._dispatch(tag, frag)

    def _dispatch(self, tag, xml_str):
        try:
            elem = ET.fromstring(xml_str)
            self._events.put({'tag': tag, 'elem': elem})
        except ET.ParseError:
            pass

    def wait_for(self, predicate, timeout=300, abort_check=None):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if abort_check and abort_check():
                log.info('wait_for: abort signalled')
                return None
            try:
                event = self._events.get(timeout=1)
                if predicate(event):
                    return event
            except queue.Empty:
                pass
        return None

    def connect_device(self):
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="CONNECTION">'
            f'<oneSwitch name="CONNECT">On</oneSwitch>'
            f'<oneSwitch name="DISCONNECT">Off</oneSwitch>'
            f'</newSwitchVector>'
        )

    def wait_ready(self, timeout=60):
        """Connect the driver, build ISO map, capture expprogram, wait for CCD_EXPOSURE."""
        time.sleep(1)
        self.connect_device()

        deadline = time.time() + timeout
        found_exposure = False
        drain_until = None

        while True:
            now = time.time()
            if drain_until:
                if now > drain_until:
                    break
            elif now > deadline:
                break

            try:
                ev = self._events.get(timeout=1)
                e = ev.get('elem')
                if e is None or e.get('device') != CAMERA_DEV:
                    continue
                tag, name = ev['tag'], e.get('name', '')

                if tag == 'defSwitchVector' and name == 'CCD_ISO':
                    for child in e:
                        label = child.get('label', '').strip()
                        sname = child.get('name', '')
                        if label and sname:
                            self._iso_map[label] = sname
                    log.info(f'ISO map: {len(self._iso_map)} values')

                elif tag == 'defSwitchVector' and name == 'expprogram':
                    # Find the currently selected mode (On switch)
                    for child in e:
                        if child.text and child.text.strip() == 'On':
                            self._expprogram = child.get('label') or child.get('name', '')
                            break
                    log.info(f'Exposure program: {self._expprogram}')

                elif tag in ('defTextVector', 'setTextVector') and name == 'expprogram':
                    for child in e:
                        if child.text:
                            self._expprogram = child.text.strip()
                    log.info(f'Exposure program: {self._expprogram}')

                elif 'battery' in name.lower():
                    for child in e:
                        if child.text and child.text.strip():
                            self._battery = child.text.strip()
                            break
                    if self._battery:
                        log.info(f'Battery: {self._battery}')

                elif tag in ('defNumberVector', 'setNumberVector') and name == 'CCD_EXPOSURE':
                    if not found_exposure:
                        found_exposure = True
                        drain_until = time.time() + 3

            except queue.Empty:
                if drain_until:
                    break

        return found_exposure

    def get_expprogram(self):
        return self._expprogram

    def get_battery(self):
        return self._battery

    # ---- INDI commands ----

    def set_ccd_info(self):
        """Send sensor dimensions from the active camera profile."""
        cfg = current_camera_cfg()
        self._send(
            f'<newNumberVector device="{CAMERA_DEV}" name="CCD_INFO">'
            f'<oneNumber name="CCD_MAX_X">{cfg["max_x"]}</oneNumber>'
            f'<oneNumber name="CCD_MAX_Y">{cfg["max_y"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE">{cfg["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE_X">{cfg["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_PIXEL_SIZE_Y">{cfg["pixel_um"]}</oneNumber>'
            f'<oneNumber name="CCD_BITSPERPIXEL">{cfg["bits"]}</oneNumber>'
            f'</newNumberVector>'
        )

    def set_capture_target_ram(self):
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="CCD_CAPTURE_TARGET">'
            f'<oneSwitch name="RAM">On</oneSwitch>'
            f'<oneSwitch name="SD Card">Off</oneSwitch>'
            f'</newSwitchVector>'
        )

    def disconnect_device(self):
        try:
            self._send(
                f'<newSwitchVector device="{CAMERA_DEV}" name="CONNECTION">'
                f'<oneSwitch name="CONNECT">Off</oneSwitch>'
                f'<oneSwitch name="DISCONNECT">On</oneSwitch>'
                f'</newSwitchVector>'
            )
            time.sleep(1)
        except Exception:
            pass

    def set_upload_local(self, directory):
        self._upload_dir = directory
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

    def set_upload_prefix(self, prefix):
        self._send(
            f'<newTextVector device="{CAMERA_DEV}" name="UPLOAD_SETTINGS">'
            f'<oneText name="UPLOAD_DIR">{self._upload_dir}</oneText>'
            f'<oneText name="UPLOAD_PREFIX">{prefix}</oneText>'
            f'</newTextVector>'
        )

    def set_iso(self, iso):
        switch = self._iso_map.get(str(iso))
        if not switch:
            log.warning(f'ISO {iso} not in map, skipping. Available: {list(self._iso_map.keys())}')
            return
        log.info(f'Setting ISO {iso} -> {switch}')
        self._send(
            f'<newSwitchVector device="{CAMERA_DEV}" name="CCD_ISO">'
            f'<oneSwitch name="{switch}">On</oneSwitch>'
            f'</newSwitchVector>'
        )

    def expose(self, seconds, abort_check=None):
        self._send(
            f'<newNumberVector device="{CAMERA_DEV}" name="CCD_EXPOSURE">'
            f'<oneNumber name="CCD_EXPOSURE_VALUE">{seconds}</oneNumber>'
            f'</newNumberVector>'
        )

        seen = [False]
        alerted = [False]

        def exposure_done(ev):
            e = ev.get('elem')
            if e is None or ev['tag'] != 'setNumberVector':
                return False
            if e.get('device') != CAMERA_DEV or e.get('name') != 'CCD_EXPOSURE':
                return False
            state = e.get('state', '')
            if state == 'Busy':
                seen[0] = True
                return False
            if state == 'Alert':
                alerted[0] = True
                return True   # stop waiting immediately on error
            return seen[0] and state == 'Ok'

        ev = self.wait_for(exposure_done, timeout=seconds + 120, abort_check=abort_check)
        if alerted[0]:
            log.warning('CCD_EXPOSURE returned Alert — invalid exposure value or camera error')
            return False
        return ev is not None


# ---- gphoto2 capture (daytime discrete shutter speeds) ----

def capture_one_frame_gphoto2(output, prefix, iso, exposure):
    """Capture one frame using gphoto2 CLI with discrete shutter speed.

    Uses gphoto2 directly — INDI driver is idle so it does not hold USB.
    Returns True on success, False on failure.
    """
    import subprocess as _subprocess

    if exposure < 1:
        ss = f'1/{round(1 / exposure)}'
    else:
        ss = str(round(exposure))

    # %n = sequential index (1, 2, …), %C = camera-chosen extension (jpg, arw, …)
    filename_tpl = os.path.join(output, f'{prefix}_%n.%C')
    cmd = [
        'gphoto2',
        '--set-config', f'shutterspeed={ss}',
        '--set-config', f'iso={iso}',
        '--capture-image-and-download',
        '--filename', filename_tpl,
        '--force-overwrite',
    ]
    log.info(f'gphoto2 capture: shutterspeed={ss} iso={iso}')
    try:
        result = _subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except _subprocess.TimeoutExpired:
        log.error('gphoto2 timed out after 60s')
        return False
    if result.returncode != 0:
        log.error(f'gphoto2 failed (rc={result.returncode}): {result.stderr.strip()}')
        return False
    log.info(f'gphoto2 output: {result.stdout.strip()}')
    return True


# ---- Capture logic ----

PREVIEW_DEFAULTS = {'frames': 1, 'exposure': 0.001, 'iso': 1600}

_capturing = False
_capture_lock = threading.Lock()
_mqtt_client = None
_last_preview_path = None
_last_fits_path = None
_last_hq_jpeg_path = None
_last_known_mode = None


def find_frame_by_id(frame_id, shots_dir='/shots'):
    """Find a frame file by ID (HHMMSS_N) — scans shots dir for matching filename.
    Frame number in filenames is zero-padded to 4 digits; the ID uses unpadded N.
    """
    import glob as _glob
    parts = frame_id.split('_')
    if len(parts) == 2:
        time_part, frame_num = parts
        search = f'{time_part}_{frame_num.zfill(4)}'
    else:
        search = frame_id
    candidates = [
        f for f in _glob.glob(os.path.join(shots_dir, f'*{search}*'))
        if not f.endswith('_preview.jpg')
    ]
    if not candidates:
        return None
    fits = [f for f in candidates if f.lower().endswith(('.fits', '.fit'))]
    return fits[0] if fits else candidates[0]


def abortable_sleep(seconds):
    """Sleep for seconds, checking abort flag every second. Returns True if completed, False if aborted."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        with _capture_lock:
            if not _capturing:
                return False
        time.sleep(min(1.0, deadline - time.time()))
    return True


def pub(subtopic, payload):
    if _mqtt_client:
        _mqtt_client.publish(f'{MQTT_PREFIX}/{subtopic}', json.dumps(payload))


def capture_one_frame(output, prefix, iso, exposure):
    """Open a fresh INDI connection, take one frame, disconnect. Returns (ok, mode)."""
    indi = INDIClient(INDI_HOST, INDI_PORT)
    try:
        indi.connect(timeout=30)
        if not indi.wait_ready(timeout=30):
            raise RuntimeError('Camera driver not ready in INDI server')
        mode = indi.get_expprogram()
        indi.set_upload_local(output)
        indi.set_upload_prefix(prefix)
        indi.set_ccd_info()
        indi.set_capture_target_ram()
        time.sleep(1)
        if iso is not None:
            indi.set_iso(iso)
        ok = indi.expose(exposure, abort_check=lambda: not _capturing)
        return ok, mode
    finally:
        indi.disconnect_device()
        indi.close()


def run_capture(params):
    global _capturing, _last_preview_path, _last_fits_path, _last_hq_jpeg_path, _last_known_mode

    output = params.get('output', '/shots')
    delay_start = float(params.get('delay_start', 0))
    delay = float(params.get('delay', 0))

    # --- Determine capture engine ---
    defaults = current_defaults()
    requested_exp = float(params.get('exposure', defaults['exposure']))
    engine = current_capture_engine(requested_exp)

    if engine == 'daytime':
        # gphoto2 path: skip INDI mode probe, assume camera is in Manual
        raw_mode = 'Manual'
        mode = 'Manual'
    else:
        # INDI/Bulb path: probe camera mode
        log.info('Probing camera mode...')
        probe = INDIClient(INDI_HOST, INDI_PORT)
        raw_mode = None
        try:
            probe.connect(timeout=15)
            probe.wait_ready(timeout=20)
            raw_mode = probe.get_expprogram()
        except Exception as e:
            log.warning(f'Mode probe failed: {e}')
        finally:
            try:
                probe.disconnect_device()
                probe.close()
            except Exception:
                pass
        mode = normalise_mode(raw_mode)
        _last_known_mode = mode

    frames, exposure, iso, ignored = resolve_capture_params(params, raw_mode)
    camera = _profile.get('camera', 'unknown')

    log.info(f'Capture: engine={engine} {frames}x mode={mode} exp={exposure}s iso={iso} ignored={ignored} -> {output}')

    pub('status', {
        'state': 'capturing', 'frame': 0, 'total': frames,
        'camera': camera, 'mode': mode, 'engine': engine,
        'exposure': exposure, 'iso': iso,
    })

    if ignored:
        mode_msg = f'{mode} mode \u2014 {", ".join(ignored)} ignored (camera controls these)'
        pub('event/info', {'message': mode_msg})
        log.info(mode_msg)

    os.makedirs(output, exist_ok=True)
    session_ts = time.strftime('%Y%m%d_%H%M%S')

    # --- Delayed start ---
    if delay_start > 0:
        pub('event/info', {'message': f'Starting in {delay_start:.0f}s\u2026'})
        pub('status', {'state': 'waiting', 'camera': camera, 'mode': mode,
                       'delay_remaining': delay_start, 'total': frames})
        if not abortable_sleep(delay_start):
            pub('event/aborted', {'frame': 0, 'total': frames})
            pub('status', {'state': 'idle', 'camera': camera})
            return

    try:
        for i in range(1, frames + 1):
            with _capture_lock:
                if not _capturing:
                    pub('event/aborted', {'frame': i - 1, 'total': frames})
                    pub('status', {'state': 'idle', 'camera': camera})
                    return

            log.info(f'Frame {i}/{frames}')
            pub('status', {'state': 'capturing', 'frame': i, 'total': frames,
                           'camera': camera, 'mode': mode})

            prefix = f'frame_{session_ts}_{i:04d}'
            frame_start = time.time()
            try:
                if engine == 'daytime':
                    ok = capture_one_frame_gphoto2(output, prefix, iso, exposure)
                else:
                    ok, _ = capture_one_frame(output, prefix, iso, exposure)
            except Exception as e:
                log.error(f'Frame {i} error: {e}')
                pub('event/error', {'message': str(e)})
                pub('status', {'state': 'idle', 'camera': camera})
                return

            if not ok:
                log.warning(f'Frame {i}: capture failed, retrying in 3s...')
                time.sleep(3)
                try:
                    if engine == 'daytime':
                        ok = capture_one_frame_gphoto2(output, prefix, iso, exposure)
                    else:
                        # Occasional Sony PTP event miss; one retry consistently recovers.
                        ok, _ = capture_one_frame(output, prefix, iso, exposure)
                except Exception as e:
                    log.error(f'Frame {i} retry error: {e}')
                    pub('event/error', {'message': str(e)})
                    pub('status', {'state': 'idle', 'camera': camera})
                    return

            if not ok:
                pub('event/error', {'message': f'Exposure timed out at frame {i}'})
                pub('status', {'state': 'idle', 'camera': camera})
                return

            # Find the file the INDI driver actually saved (it appends its own counter)
            # and extract JPEG preview for /astro last.
            frame_file = find_latest_frame(output, frame_start)
            log.info(f'Frame {i} saved as: {frame_file}')
            # ID derived from filename: HHMMSS_N — persistent, no registry needed
            time_part = session_ts.split('_')[1]   # HHMMSS from YYYYMMDD_HHMMSS
            frame_id = f'{time_part}_{i}'
            jpeg_path = None
            hq_path = None
            if frame_file:
                if frame_file.lower().endswith(('.fits', '.fit')):
                    _last_fits_path = frame_file
                    jpeg_path, hq_path = extract_jpeg_previews(frame_file)
                    _last_hq_jpeg_path = hq_path
                else:
                    jpeg_path = frame_file  # already a JPEG
                if jpeg_path:
                    _last_preview_path = jpeg_path
                    pub('event/preview', {'path': jpeg_path, 'id': frame_id})

            pub('event/frame', {'frame': i, 'total': frames, 'id': frame_id})

            # --- Inter-frame delay ---
            if delay > 0 and i < frames:
                pub('event/info', {'message': f'Frame {i}/{frames} done \u00b7 next in {delay:.0f}s'})
                pub('status', {'state': 'waiting', 'camera': camera, 'mode': mode,
                               'frame': i, 'total': frames})
                if not abortable_sleep(delay):
                    pub('event/aborted', {'frame': i, 'total': frames})
                    pub('status', {'state': 'idle', 'camera': camera})
                    return

        pub('event/complete', {'frames': frames, 'camera': camera, 'mode': mode,
                               'exposure': exposure, 'iso': iso, 'id': frame_id})
        pub('status', {'state': 'idle', 'camera': camera})
        log.info('Session complete')
    finally:
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

    elif topic == 'command/profile':
        camera = payload.get('camera', '').lower()
        if camera not in CAMERA_PROFILES:
            pub('event/error', {
                'message': f'Unknown camera: {camera}. Available: {", ".join(CAMERA_PROFILES)}'
            })
            return
        _profile['camera'] = camera
        save_profile()
        log.info(f'Profile set: camera={camera}')
        pub('event/profile', {'camera': camera, **CAMERA_PROFILES[camera]})
        pub('status', {'state': 'capturing' if _capturing else 'idle',
                       'camera': camera})

    elif topic == 'command/defaults':
        defaults = _profile.get('defaults', dict(_PROFILE_DEFAULTS['defaults']))
        for key in ('frames', 'exposure', 'iso'):
            if key in payload:
                defaults[key] = payload[key]
        _profile['defaults'] = defaults
        save_profile()
        log.info(f'Defaults updated: {defaults}')
        pub('event/defaults', defaults)

    elif topic == 'command/engine':
        engine = payload.get('engine', '').lower()
        if engine not in ('auto', 'star', 'daytime'):
            pub('event/error', {
                'message': f'Unknown engine: {engine}. Use: auto, star, daytime'
            })
            return
        _profile['capture_engine'] = engine
        save_profile()
        log.info(f'Capture engine set to: {engine}')
        pub('event/info', {'message': f'Capture engine: {engine}'})

    elif topic == 'query/status':
        camera = _profile.get('camera', 'unknown')
        pub('status', {
            'state': 'capturing' if _capturing else 'idle',
            'camera': camera,
            'mode': _last_known_mode,
            'engine': _profile.get('capture_engine', 'auto'),
            'defaults': current_defaults(),
            'last_preview': _last_preview_path,
        })

    elif topic == 'query/last':
        preview = _last_preview_path
        if not preview:
            # Bridge restarted — find most recent preview JPEG on disk
            import glob as _glob
            previews = _glob.glob('/shots/*_preview.jpg')
            if previews:
                preview = max(previews, key=os.path.getmtime)
        if preview and os.path.exists(preview):
            pub('event/preview', {'path': preview})
        else:
            pub('event/error', {'message': 'No preview available'})

    elif topic == 'query/archive':
        frame_id = (payload.get('id') or 'last').lstrip('#')
        if frame_id == 'last':
            path = _last_fits_path or _last_preview_path
            if not path:
                # Bridge restarted — find most recent FITS on disk
                import glob as _glob
                fits = _glob.glob('/shots/*.fits') + _glob.glob('/shots/*.fit')
                if fits:
                    path = max(fits, key=os.path.getmtime)
        else:
            path = find_frame_by_id(frame_id)
            if not path:
                pub('event/error', {'message': f'No frame found for #{frame_id}'})
                return
        if path and os.path.exists(path):
            payload = {
                'path': path,
                'filename': os.path.basename(path),
                'size_mb': round(os.path.getsize(path) / 1048576, 1),
                'id': frame_id,
            }
            hq = os.path.splitext(path)[0] + '_hq.jpg'
            if os.path.exists(hq):
                payload['hq_filename'] = os.path.basename(hq)
                payload['hq_size_mb'] = round(os.path.getsize(hq) / 1048576, 1)
            pub('event/archive', payload)
        else:
            pub('event/error', {'message': 'No image to archive'})

    elif topic == 'query/limits':
        cfg = current_camera_cfg()
        camera = _profile.get('camera', 'unknown')
        pub('event/limits', {
            'camera': camera,
            'min_exposure': cfg['min_exposure'],
            'max_exposure': cfg['max_exposure'],
            'min_iso': cfg['min_iso'],
            'max_iso': cfg['max_iso'],
            'resolution': f"{cfg['max_x']}×{cfg['max_y']}",
            'pixel_um': cfg['pixel_um'],
            'bits': cfg['bits'],
        })

    elif topic == 'command/preview':
        with _capture_lock:
            if _capturing:
                pub('event/error', {'message': 'Already capturing'})
                return
            _capturing = True
        preview_params = dict(PREVIEW_DEFAULTS)
        preview_params.update(payload)
        preview_params['frames'] = 1   # always single frame
        threading.Thread(target=run_capture, args=(preview_params,), daemon=True).start()

    elif topic == 'query/battery':
        def _battery_query():
            indi = INDIClient(INDI_HOST, INDI_PORT)
            try:
                indi.connect(timeout=15)
                indi.wait_ready(timeout=20)
                level = indi.get_battery()
                global _last_known_mode
                _last_known_mode = normalise_mode(indi.get_expprogram())
                pub('event/battery', {'level': level or 'unknown'})
            except Exception as e:
                pub('event/error', {'message': f'Battery query failed: {e}'})
            finally:
                try:
                    indi.disconnect_device()
                    indi.close()
                except Exception:
                    pass
        threading.Thread(target=_battery_query, daemon=True).start()


def on_connect(client, userdata, connect_flags, reason_code, properties):
    if not reason_code.is_failure:
        log.info(f'MQTT connected to {MQTT_HOST}:{MQTT_PORT}')
        client.subscribe(f'{MQTT_PREFIX}/command/#')
        client.subscribe(f'{MQTT_PREFIX}/query/#')
        camera = _profile.get('camera', 'unknown')
        pub('status', {'state': 'idle', 'camera': camera})
    else:
        log.error(f'MQTT connect failed: {reason_code}')


def main():
    global _mqtt_client
    load_profile()
    _mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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
