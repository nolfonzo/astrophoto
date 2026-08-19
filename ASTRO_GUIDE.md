# Astrophotography System Guide

Control your Sony camera remotely via Telegram bot (`@dangarisland_bot`).

All commands start with `/astro`. The camera lives on the Pi 4 at Dangar Island; commands flow through Node-RED on the LXD server.

---

## Contents

- [User Manual](#user-manual)
  - [Camera setup](#1-camera-setup)
  - [Taking a test shot](#2-taking-a-test-shot)
  - [Capture sessions](#3-capture-sessions)
  - [Timelapse — basic](#4-timelapse--basic)
  - [Timelapse — profiles](#5-timelapse--profiles)
  - [Timelapse — exposure ramping](#6-timelapse--exposure-ramping)
  - [Managing custom profiles](#7-managing-custom-profiles)
  - [Reviewing and archiving shots](#8-reviewing-and-archiving-shots)
  - [Rendering a timelapse video](#9-rendering-a-timelapse-video)
  - [Tips and best practices](#10-tips-and-best-practices)
- [Reference Guide](#reference-guide)
  - [All commands](#all-commands)
  - [Built-in timelapse profiles](#built-in-timelapse-profiles)
  - [Parameter reference](#parameter-reference)
  - [Common workflows](#common-workflows)

---

# User Manual

## 1. Camera setup

Before capturing, check the camera is connected and set to the right model:

```
/astro status          — check current state
/astro camera          — show active camera, limits, defaults
/astro camera a6400    — switch to a6400 (or a6700)
```

The camera must be in **Manual** mode for full control of exposure and ISO. In Aperture or Shutter Priority modes, the camera ignores exposure commands (but still obeys ISO).

**Battery check:**
```
/astro battery
```

---

## 2. Taking a test shot

Before committing to a long session, take a quick preview to check framing and focus:

```
/astro preview                         — 1/1000s at ISO 1600 (daylight check)
/astro preview exposure=2 iso=3200     — 2s at ISO 3200 (night check)
```

The preview is sent directly to Telegram as a JPEG. If it looks good, you're ready to shoot.

---

## 3. Capture sessions

A single capture takes one or more frames with fixed settings:

```
/astro capture                                    — 1 frame at current profile settings
/astro capture frames=10                          — 10 frames
/astro capture frames=20 exposure=90 iso=3200     — deep sky: 20 × 90s at ISO 3200
```

Each frame is saved as an ARW (Sony RAW) on the Pi. A JPEG preview is sent to Telegram after each frame.

**Checking on a running capture:**
```
/astro status     — shows current frame count
```

**Stopping early:**
```
/astro abort
```

**Profile** stores your default settings (frames, exposure, ISO) so you don't have to type them every time:
```
/astro profile                                — show current profile
/astro profile frames=20 exposure=90 iso=3200 — update profile
/astro defaults load                          — reset profile to camera defaults
```

---

## 4. Timelapse — basic

A timelapse takes frames at a regular interval. The key parameter is `interval` — the time **from the start of one frame to the start of the next** (not the gap between frames).

```
/astro timelapse interval=10 frames=120 exposure=1 iso=400
```

This takes 120 frames, one every 10 seconds (20 minutes total).

**Using duration instead of frames:**
```
/astro timelapse interval=6 duration=5400 exposure=0.5 iso=100
```
`duration=5400` = 90 minutes. Frame count is calculated automatically (900 frames).

**Interval tips:**
- 5–6s for fast-moving clouds or golden hour
- 10–15s for general day/night transitions
- 25–30s for star trails and long night shoots
- Keep `interval > exposure + ~2s` for camera processing time

When a timelapse starts, the bot sends a **run ID** (`YYYYMMDD_HHMMSS`) — save this, you'll need it to render the video later.

---

## 5. Timelapse — profiles

Instead of specifying all parameters manually, use a named profile:

```
/astro timelapse profile=timelapse-sunset
```

**Listing available profiles:**
```
/astro profiles
```

### Naming convention

A profile whose name starts with `timelapse-` runs a **sequence** — it carries
an interval and a duration. Any other name is a **single shot**. The name tells
you which you are getting, so `/astro capture profile=deep-sky` cannot
accidentally start a three-hour run.

Profiles work with both `/astro capture` and `/astro timelapse`.

**Presets** are a different thing: they snapshot the active camera settings
(frames, exposure, ISO) with no timing at all.

### Built-in profiles

| Profile | Duration | Interval | ISO | Exposure | Notes |
|---|---|---|---|---|---|
| `timelapse-sunset` | 90 min | 6s | 100→3200 | 0.5s→4s | Ramps 50 min, holds 40 min |
| `timelapse-sunrise` | 90 min | 6s | 3200→100 | 4s→0.5s | Ramps 50 min, holds 40 min |
| `timelapse-blue-hour` | 40 min | 6s | 400→1600 | 1s→3s | Ramps 20 min, holds 20 min |
| `timelapse-night-sky` | 2 hrs | 30s | 3200 fixed | 15s | Fixed, no ramp |
| `timelapse-stars` | 3 hrs | 30s | 6400 fixed | 20s | Fixed, no ramp |
| `timelapse-golden-hour` | 1 hr | 5s | 100 fixed | 0.01s | Short, auto-ish |

You can override any profile parameter on the command line:
```
/astro timelapse profile=timelapse-night-sky duration=10800    — extend to 3 hours
/astro timelapse profile=timelapse-sunset ramp_duration=2400   — ramp over 40 min instead of 50
```

---

## 6. Timelapse — exposure ramping

For twilight transitions, the system ramps **ISO and exposure logarithmically** across the timelapse — matching how light actually changes (exponentially, not linearly).

### How it works

- `iso_start` / `iso_end` — ISO at frame 1 and last ramp frame
- `exposure_start` / `exposure_end` — exposure time at frame 1 and last ramp frame
- `ramp_duration` — how many seconds the ramp runs before holding at the end values

The ramp follows a log curve: doubling ISO takes the same number of frames whether you're going from 100→200 or 1600→3200. This matches real-world light behaviour and produces smooth transitions.

**After `ramp_duration` elapses**, ISO and exposure are clamped at their end values for the rest of the timelapse. So for `timelapse-sunset`, the first 50 minutes ramp from daylight to dark settings, then the camera holds at ISO 3200 / 4s for the final 40 minutes of full darkness.

### Ramping manually (without a profile)

```
/astro timelapse interval=6 duration=5400 iso_start=100 iso_end=3200 exposure_start=0.5 exposure_end=4 ramp_duration=3000
```

### ISO-only ramp (fixed exposure)

```
/astro timelapse interval=10 duration=3600 exposure=2 iso_start=200 iso_end=3200
```

### The 500 rule reminder

For night sections with stars, keep exposure ≤ `500 ÷ focal_length_mm` to avoid star trails in individual frames. For a 20mm lens: max ~25s.

---

## 7. Managing custom profiles

Save your own profiles to avoid retyping parameters:

```
/astro profile add <name> key=val key=val ...
```

**Examples:**
```
/astro profile add my-sunset interval=8 duration=7200 iso_start=200 iso_end=1600 exposure_start=1 exposure_end=6 ramp_duration=3600

/astro profile add storm-clouds interval=4 duration=3600 iso=400 exposure=0.01

/astro profile add deepsky-session interval=30 duration=14400 iso=6400 exposure=25
```

**Deleting a custom profile:**
```
/astro profile rm my-sunset
```

Built-in profiles (sunset, sunrise, etc.) cannot be deleted — only custom ones.

---

## 8. Reviewing and archiving shots

Shots live on the Pi until you archive them to the LXD server (`/media/astrophoto`).

**See what's on the Pi:**
```
/astro sessions                     — list sessions (grouped by date)
/astro shots 20260401               — thumbnails + frame IDs for a session
```

**Archive to LXD:**
```
/astro archive session 20260401     — copy entire session to LXD archive
/astro archive last                 — archive the most recent frame
/astro archive <id>                 — archive a specific frame by ID
```

**Browse the LXD archive:**
```
/astro archived                     — list all archived sessions
/astro archived 20260401            — list frames for a specific session
```

The archive URL is also shown as a clickable link in the response.

**Get the last frame:**
```
/astro last     — resend the most recent frame preview to Telegram
```

**Clean up the Pi:**
```
/astro delete session 20260401           — delete a session (asks for confirm)
/astro delete session 20260401 confirm   — actually delete
/astro delete all confirm                — wipe everything from Pi
```

---

## 9. Rendering a timelapse video

Once frames are archived to LXD, render them into an MP4:

```
/astro render 20260401_213000
```

The run ID (`YYYYMMDD_HHMMSS`) is shown when the timelapse starts. If you missed it, `/astro archived <session>` lists all run IDs in an archived session.

The render:
- Uses the `_preview.jpg` version of each frame (already JPEG-converted)
- Sorts frames by filename (chronological)
- Outputs 24fps H.264 MP4 via ffmpeg
- Returns a download link when done

Rendering 900 frames takes a few minutes. The output file is named `timelapse_YYYYMMDD_HHMMSS.mp4` in the archive.

---

## 10. Tips and best practices

**For twilight (holy grail) timelapses:**
- Start the timelapse ~5–10 min before you want ramping to begin (the `timelapse-sunset` profile runs for 90 min — start it at golden hour)
- `timelapse-blue-hour` is better if you only want the 40-minute twilight window without the full golden hour buildup
- Cloudy nights will have the ramp complete faster — you can always shorten `ramp_duration`

**For night sky:**
- Camera in Manual mode is essential — Auto ISO or Auto shutter will fight the ramp
- Check battery with `/astro battery` before a long shoot
- `timelapse-night-sky` (30s interval) gives you 240 frames/2 hrs — ~10 seconds of video at 24fps
- `timelapse-stars` (30s interval, 3 hrs) gives ~360 frames — ~15 seconds of video

**Post-processing:**
- The rendered MP4 is a direct stitch of JPEGs at 24fps
- For de-flickering, colour grading, or LUT work, import the individual ARW files from the archive into Lightroom or DaVinci Resolve
- LRTimelapse is the gold standard for holy grail deflicker if you want silky-smooth transitions

**Checking mid-capture:**
- `/astro status` shows current frame and total
- Frame previews are sent to Telegram as they're captured
- `/astro last` resends the most recent preview if you missed it

---

# Reference Guide

## All commands

> `/astro track` exists but is a stub — it replies "Mount not connected yet".
> Star Adventurer 2i integration is not implemented.


### Camera & Profile

| Command | Description |
|---|---|
| `/astro status` | Current state, camera, active profile |
| `/astro camera` | Camera info, limits, defaults, active profile |
| `/astro camera a6400\|a6700` | Switch active camera |
| `/astro battery` | Battery level |
| `/astro limits` | Camera exposure and ISO limits |
| `/astro isos` | List valid ISO values |
| `/astro exposures` | List valid shutter speeds |
| `/astro profile` | Show active profile |
| `/astro profile frames=N exposure=S iso=N` | Update active profile |
| `/astro defaults` | Show camera defaults |
| `/astro defaults frames=N exposure=S iso=N` | Save new camera defaults |
| `/astro defaults load` | Reset active profile to camera defaults |

### Presets (named profile snapshots)

| Command | Description |
|---|---|
| `/astro preset list` | List saved presets |
| `/astro preset save <name>` | Save current profile as preset |
| `/astro preset load <name>` | Load preset into active profile |
| `/astro preset show <name>` | Show preset settings |
| `/astro preset delete <name>` | Delete a preset |

### Capture

| Command | Description |
|---|---|
| `/astro capture` | One frame at profile settings |
| `/astro capture [frames=N] [exposure=S] [iso=N]` | Capture with overrides |
| `/astro preview [exposure=S] [iso=N]` | Quick single-frame preview |
| `/astro last` | Resend last preview to Telegram |
| `/astro abort` | Stop current capture |

### Timelapse

| Command | Description |
|---|---|
| `/astro timelapse profile=<name>` | Run a named timelapse profile |
| `/astro timelapse profile=<name> duration=S` | Profile with any parameter overridden |
| `/astro timelapse interval=S [frames=N\|duration=S] [exposure=S] [iso=N]` | Manual timelapse |
| `/astro timelapse ... iso_start=N iso_end=N exposure_start=S exposure_end=S [ramp_duration=S]` | With exposure ramp |
| `/astro profiles` | List all timelapse profiles (built-in + custom) |
| `/astro profile add <name> key=val ...` | Save a custom timelapse profile |
| `/astro profile rm <name>` | Delete a custom profile |

### Sessions, Archive, Render

| Command | Description |
|---|---|
| `/astro sessions` | List sessions on Pi |
| `/astro shots <YYYYMMDD>` | Frame list + thumbnails for session |
| `/astro archive session <YYYYMMDD>` | Archive session to LXD |
| `/astro archive last` | Archive most recent frame |
| `/astro archive <id>` | Archive frame by ID |
| `/astro archived [session]` | Browse LXD archive |
| `/astro render <YYYYMMDD_HHMMSS>` | Render timelapse run to MP4 |
| `/astro delete session <YYYYMMDD> [confirm]` | Delete session from Pi |
| `/astro delete all [confirm]` | Delete all shots from Pi |

---

## Built-in timelapse profiles

| Profile | Duration | Interval | ISO | Exposure | Ramp |
|---|---|---|---|---|---|
| `timelapse-sunset` | 90 min | 6s | 100 → 3200 | 0.5s → 4s | 50 min, then hold |
| `timelapse-sunrise` | 90 min | 6s | 3200 → 100 | 4s → 0.5s | 50 min, then hold |
| `timelapse-blue-hour` | 40 min | 6s | 400 → 1600 | 1s → 3s | 20 min, then hold |
| `timelapse-night-sky` | 2 hrs | 30s | 3200 | 15s | Fixed |
| `timelapse-stars` | 3 hrs | 30s | 6400 | 20s | Fixed |
| `timelapse-golden-hour` | 1 hr | 5s | 100 | ~0.01s | Fixed |

All durations and ramp windows can be overridden per-command.

---

## Parameter reference

### Timelapse parameters

| Parameter | Type | Description |
|---|---|---|
| `profile` | string | Named profile to use (overridden by any explicit params) |
| `frames` | integer | Number of frames to capture |
| `duration` | seconds | Total duration — auto-calculates frames if frames not given |
| `interval` | seconds | Frame-start to frame-start timing (must be > exposure + ~2s) |
| `exposure` | seconds | Shutter speed per frame (use fractions: `0.5`, `1/30`) |
| `iso` | integer | Fixed ISO for all frames |
| `iso_start` | integer | ISO at frame 1 (ramp start) |
| `iso_end` | integer | ISO at last ramp frame |
| `exposure_start` | seconds | Exposure at frame 1 (ramp start) |
| `exposure_end` | seconds | Exposure at last ramp frame |
| `ramp_duration` | seconds | How long to ramp before holding at end values. `0` = ramp full duration |
| `delay_start` | seconds | Wait before starting capture |

### Ramp behaviour

- ISO ramp is **logarithmic** — each stop takes the same number of frames regardless of absolute value
- Exposure ramp is also logarithmic — halving from 4s→2s takes the same frames as 2s→1s
- ISO values are snapped to the nearest standard stop: 100, 125, 160, 200, 250, 320, 400, 500, 640, 800, 1000, 1250, 1600, 2000, 2500, 3200, 4000, 5000, 6400, 8000, 10000, 12800, 25600
- Once `ramp_duration` elapses, ISO and exposure hold at their end values

### Custom profile keys

Any timelapse parameter above can be stored in a custom profile:

```
/astro profile add milky-way interval=25 duration=7200 iso=6400 exposure=20 delay_start=300
```

---

## Common workflows

### Sunset timelapse (holy grail)

1. Set up camera, point west, focus at infinity
2. Start 5–10 min before golden hour:
   ```
   /astro timelapse profile=timelapse-sunset
   ```
3. Note the run ID from the bot reply
4. After 90 min, archive the session:
   ```
   /astro archive session <YYYYMMDD>
   ```
5. Render:
   ```
   /astro render <run_id>
   ```

### Star field timelapse

1. Camera in Manual, ISO 6400, widest aperture, focus at infinity
2. Check 500-rule: for a 20mm lens, max ~25s exposure
   ```
   /astro timelapse profile=timelapse-stars
   ```
3. Or with 500-rule compliant exposure for your lens:
   ```
   /astro timelapse interval=30 duration=10800 iso=6400 exposure=20
   ```

### Deep sky single capture

```
/astro profile frames=30 exposure=120 iso=3200
/astro capture
```

### Quick test before a session

```
/astro preview exposure=30 iso=3200
```

Check the preview for framing, focus, and that no light pollution is ruining the frame.

### Custom twilight for a specific location/season

The built-in `timelapse-sunset` profile ramps for 50 minutes. If your twilight is faster (tropics) or slower (high latitude summer), adjust:

```
/astro profile add tropical-sunset interval=6 duration=4200 iso_start=100 iso_end=3200 exposure_start=0.5 exposure_end=4 ramp_duration=2400
```

Then use it:
```
/astro timelapse profile=tropical-sunset
```
