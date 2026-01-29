#!/bin/bash
# Deploy script for MAUDE Analyzer
# This script builds the frontend and deploys it to the static directory
# Run this after making frontend changes to ensure they are visible in the app

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== MAUDE Analyzer Deploy Script ==="
echo ""

# Step 1: Build frontend
echo "1. Building frontend..."
cd "$PROJECT_ROOT/frontend"

# Check if node is available
if command -v npm &> /dev/null; then
    npm run build
elif [ -f "$HOME/local/node-v20.11.0-darwin-x64/bin/npm" ]; then
    export PATH="$HOME/local/node-v20.11.0-darwin-x64/bin:$PATH"
    npm run build
else
    echo "Error: npm not found. Please install Node.js or set up your PATH."
    exit 1
fi

echo "   Build complete!"

# Step 2: Deploy to static directory
echo ""
echo "2. Deploying to static directory..."
cd "$PROJECT_ROOT"

# Remove old static files
rm -rf static/*

# Copy new build
cp -r frontend/dist/* static/

echo "   Deployed!"

# Step 3: Show what was deployed
echo ""
echo "3. Deployed files:"
ls -la static/
ls -la static/assets/

# Step 4: Restart server if running
echo ""
echo "4. Checking server..."
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "   Server is running. Restart it to see changes:"
    echo "   $ lsof -ti:8000 | xargs kill -9"
    echo "   $ ./venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000"
else
    echo "   No server running on port 8000."
fi

echo ""
echo "=== Deploy complete! ==="
echo ""
echo "To start the app:"
echo "  cd $PROJECT_ROOT"
echo "  ./venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000"
echo ""
echo "Then open: http://localhost:8000"
