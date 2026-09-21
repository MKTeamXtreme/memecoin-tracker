#!/bin/bash

echo ""
echo "  ============================================"
echo "    Solana Memecoin Tracker -- Trial Run"
echo "  ============================================"
echo ""

# Kill any old server on port 8000
echo "  Clearing port 8000..."
fuser -k 8000/tcp >/dev/null 2>&1
echo "  Port cleared."
echo ""

# Install requirements
echo "  Installing / checking dependencies..."
pip3 install fastapi uvicorn aiohttp -q --disable-pip-version-check
echo "  Done."
echo ""

# Open browser (tries different linux commands)
(sleep 3 && (xdg-open http://localhost:8000 >/dev/null 2>&1 || true)) &

echo "  Starting server at http://localhost:8000"
echo "  Press Ctrl+C to stop."
echo ""

python3 main.py

echo ""
echo "  ============================================"
echo "    Server stopped."
echo "  ============================================"
echo ""
