#!/bin/bash
set -e

echo "Starting INDI server..."
echo "Detected USB devices:"
lsusb 2>/dev/null || echo "  (none or lsusb not available)"

# Start INDI server with gphoto driver for Sony camera
# -v verbose, -p port, drivers listed after --
exec indiserver -v -p 7624 indi_gphoto_ccd
