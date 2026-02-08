#!/bin/bash
#
# Weekly FDA MAUDE Data Refresh Script
# Run via cron every Friday at 6 AM (after FDA's Thursday release)
#
# Cron entry (add with: crontab -e)
# 0 6 * * 5 /Users/alexbenison/maude-analyzer/scripts/cron_weekly_refresh.sh >> /Users/alexbenison/maude-analyzer/logs/weekly_refresh.log 2>&1
#

set -e

# Configuration
PROJECT_DIR="/Users/alexbenison/maude-analyzer"
PYTHON="/usr/bin/python3"
LOG_DIR="${PROJECT_DIR}/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create log directory if needed
mkdir -p "${LOG_DIR}"

echo "========================================"
echo "FDA MAUDE Weekly Refresh"
echo "Started: $(date)"
echo "========================================"

cd "${PROJECT_DIR}"

# Step 1: Download latest ADD files from FDA
echo ""
echo "Step 1: Downloading latest ADD files..."
for file in mdrfoiAdd foidevAdd foitextAdd patientAdd; do
    echo "  Downloading ${file}.zip..."
    curl -s -o "data/raw/${file}.zip" "https://www.accessdata.fda.gov/MAUDE/ftparea/${file}.zip"
    unzip -o -q "data/raw/${file}.zip" -d data/raw/
    rm "data/raw/${file}.zip"
done
echo "  Download complete."

# Step 2: Stop API server if running (to release database lock)
echo ""
echo "Step 2: Stopping API server..."
pkill -f "uvicorn api.main:app" 2>/dev/null || echo "  API server not running"
sleep 2

# Step 3: Load ADD files into database
echo ""
echo "Step 3: Loading ADD files..."
${PYTHON} -c "
from pathlib import Path
from src.ingestion.loader import MAUDELoader

loader = MAUDELoader(
    db_path=Path('data/maude.duckdb'),
    batch_size=10000,
    enable_transaction_safety=True,
    enable_validation=True,
)

files = [
    ('data/raw/mdrfoiAdd.txt', 'master'),
    ('data/raw/foidevAdd.txt', 'device'),
    ('data/raw/foitextAdd.txt', 'text'),
    ('data/raw/patientAdd.txt', 'patient'),
]

for filepath, file_type in files:
    print(f'  Loading {filepath}...')
    result = loader.load_file(Path(filepath), file_type=file_type)
    print(f'    Loaded: {result.records_loaded:,} records')

print('  Load complete.')
"

# Step 4: Restart API server
echo ""
echo "Step 4: Restarting API server..."
cd "${PROJECT_DIR}"
nohup ${PYTHON} -m uvicorn api.main:app --host 127.0.0.1 --port 8000 > "${LOG_DIR}/api_server.log" 2>&1 &
sleep 3
echo "  API server started (PID: $!)"

# Step 5: Run quality gate
echo ""
echo "Step 5: Running quality gate..."
${PYTHON} scripts/quality_gate.py 2>&1 | tail -20

echo ""
echo "========================================"
echo "Weekly refresh complete!"
echo "Finished: $(date)"
echo "========================================"
