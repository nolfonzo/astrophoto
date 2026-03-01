#!/bin/bash
# Simple gphoto2 capture script for Sony a6400
# Usage: ./capture.sh [frames] [exposure_secs] [iso] [output_dir]

FRAMES=${1:-10}
EXPOSURE=${2:-60}
ISO=${3:-1600}
OUTPUT=${4:-/shots}
INTERVAL=$((EXPOSURE + 5))   # 5s gap between frames

mkdir -p "$OUTPUT"

echo "=== Astrophoto Capture ==="
echo "Frames:   $FRAMES"
echo "Exposure: ${EXPOSURE}s (bulb)"
echo "ISO:      $ISO"
echo "Output:   $OUTPUT"
echo "=========================="

# Detect camera
docker exec indiserver gphoto2 --auto-detect

# Set camera params and shoot
docker exec indiserver gphoto2 \
    --set-config iso=$ISO \
    --set-config shutterspeed=bulb \
    --frames=$FRAMES \
    --interval=$INTERVAL \
    --bulb=$EXPOSURE \
    --capture-image-and-download \
    --filename "$OUTPUT/%Y%m%d_%H%M%S.arw"

echo "Done. Images saved to $OUTPUT"
