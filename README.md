# MAUDE Database Analyzer

A local desktop application for analyzing FDA MAUDE (Manufacturer and User Facility Device Experience) database records, with initial focus on **Spinal Cord Stimulation (SCS) devices**.

## Features

- **Unlimited Search**: No result limits (unlike FDA's 500 record limit)
- **Full Data Access**: All FDA MAUDE fields available
- **Interactive Visualizations**: Tableau-like charts and dashboards
- **Manufacturer Comparison**: Side-by-side analysis of Abbott, Medtronic, Boston Scientific, Nevro, etc.
- **Export Capabilities**: CSV, Excel, JSON, Parquet
- **Local First**: All data stored locally, no subscription fees

## Technology Stack

| Component | Technology |
|-----------|------------|
| Database | DuckDB |
| Frontend | Streamlit |
| Visualization | Plotly |
| Language | Python 3.11+ |

## Quick Start

### 1. Prerequisites

- Python 3.11 or higher
- ~100GB free disk space (for full MAUDE database)

### 2. Installation

```bash
# Clone or navigate to the project directory
cd maude-analyzer

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env
```

### 3. Download FDA Data

See `data/raw/README.md` for instructions on downloading FDA MAUDE files.

### 4. Load Data

```bash
# Run initial data load (after downloading FDA files)
python scripts/initial_load.py
```

### 5. Run Application

```bash
# Start Streamlit app
streamlit run app/main.py
```

The application will open in your browser at `http://localhost:8501`

## Project Structure

```
maude-analyzer/
├── config/             # Configuration files
├── data/
│   ├── raw/            # Downloaded FDA files
│   ├── processed/      # Intermediate files
│   ├── lookups/        # Static lookup tables
│   └── maude.duckdb    # Main database
├── src/
│   ├── ingestion/      # Data loading pipeline
│   ├── database/       # DuckDB operations
│   ├── analysis/       # Analysis functions
│   ├── visualization/  # Chart generation
│   └── utils/          # Utility functions
├── app/
│   ├── pages/          # Streamlit pages
│   └── components/     # Reusable UI components
├── scripts/            # Utility scripts
├── tests/              # Test suite
└── docs/               # Documentation
```

## Configuration

Edit `.env` file to customize settings:

```bash
# Database location
MAUDE_DB_PATH=./data/maude.duckdb

# FDA API key (optional, for automated updates)
FDA_API_KEY=your_key_here

# Application settings
LOG_LEVEL=INFO
QUERY_TIMEOUT_SECONDS=60
```

## Initial Scope

### Product Codes
- **GZB**: Spinal Cord Stimulator (primary)
- **LGW**: Spinal Cord Stimulator (older systems)
- **PMP**: DRG Stimulators

### Key Manufacturers
- Abbott (including St. Jude Medical)
- Medtronic
- Boston Scientific
- Nevro
- Stimwave
- Nalu Medical
- Saluda Medical

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Code Formatting

```bash
black .
ruff check .
```

## Data Sources

- **Primary**: [FDA MAUDE Downloads](https://www.fda.gov/medical-devices/maude-database/download-maude-data)
- **Updates**: [openFDA API](https://open.fda.gov/apis/device/event/)

## License

Private - Internal Use Only

## Author

Alex - Abbott Neuromodulation
