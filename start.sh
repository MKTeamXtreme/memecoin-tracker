#!/bin/bash
echo "============================================"
echo "  Solana Memecoin Tracker -- Trial Run"
echo "============================================"

echo "Clearing port 8000..."
lsof -ti:8000 | xargs kill -9 2>/dev/null
echo "Port cleared."

echo "Installing / checking dependencies..."
pip3 install fastapi uvicorn aiohttp -q --disable-pip-version-check
echo "Done."

echo "Starting server at http://localhost:8000"
echo "Press Ctrl+C to stop."

(sleep 3 && open http://localhost:8000) &

python3 main.py
