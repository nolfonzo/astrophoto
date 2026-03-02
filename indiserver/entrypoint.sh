#!/bin/bash
set -e

echo "Detected USB devices:"
lsusb 2>/dev/null || echo "  (none or lsusb not available)"

echo "Starting capture service on port 7625..."
exec python3 /capture_service.py
