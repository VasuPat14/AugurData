# Indicator Calculation Script (`indicators.py`)

A high-performance command-line utility for calculating technical indicators (VWAP, 5/9/20/50/200 EMAs) on stock data.

## Features

- **In-Place Updates** – Modifies data files directly, adding new columns for each indicator
- **Atomic Writes** – Uses temporary files and replace-on-success strategy to prevent data corruption
- **Dual Processing Modes** – Bulk mode for efficiency or manual mode for one-off tasks
- **Smart Resume** – Automatically skips tickers that already have indicator data
- **Gzipped CSV Support** – Reads and writes `.csv.gz` files directly

## Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd <your-repo-name>

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Syntax

```bash
python indicators.py <directory> [OPTIONS]
```

### Command-Line Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `directory` | **Required** | Root directory containing `.csv.gz` data files |
| **Ticker Source** | **(Choose One)** | |
| `-t`, `--ticker` | Optional | One or more ticker symbols to process |
| `--all-tickers` | Optional | Process every ticker found in target files |
| **File/Date Mode** | **(Choose One)** | |
| `--bulk-file` | Optional | Path to JSON file for bulk processing |
| `-f`, `--file` | Optional | Process a single data file |
| `-y`, `--year` | Optional | Process all files in a specific year |
| `-a`, `--all` | Optional | Process all `.csv.gz` files in directory |
| **Filters** | **(Optional)** | |
| `-m`, `--month` | Optional | Specific month (1-12), requires `-y` |
| `--start-date` | Optional | Start date in YYYY-MM-DD format |
| `--end-date` | Optional | End date in YYYY-MM-DD format |

## Processing Modes

### 1. Bulk Mode (Recommended)

Most efficient for processing multiple tickers across different date ranges.

**Input File Format (`requests.json`):**

```json
[
  "2024-01-02 2024-01-05 AAPL",
  "2024-01-03 2024-01-03 MSFT",
  "2024-01-08 2024-01-10 GOOGL",
  "2024-02-01 2024-02-28 TSLA"
]
```

Each line follows the format: `"START_DATE END_DATE TICKER"`

**Command:**

```bash
python indicators.py L:\minutes --bulk-file C:\data\requests.json
```

### 2. Manual Mode

For one-off processing tasks.

#### Process Specific Tickers

**Single file:**
```bash
python indicators.py L:\minutes -t AAPL MSFT -f 2024-01-02.csv.gz
```

**Entire year:**
```bash
python indicators.py L:\minutes -t AAPL -y 2024
```

**Specific month:**
```bash
python indicators.py L:\minutes -t TSLA -y 2024 -m 2
```

**Custom date range:**
```bash
python indicators.py L:\minutes -t TSLA AAPL MSFT --start-date 2024-02-01 --end-date 2024-02-05 -a
```

**Date range within a year:**
```bash
python indicators.py L:\minutes -t NVDA -y 2024 --start-date 2024-03-01 --end-date 2024-03-05
```

#### Process All Tickers

**Single file:**
```bash
python indicators.py L:\minutes --all-tickers -f 2024-01-03.csv.gz
```

**Entire year:**
```bash
python indicators.py L:\minutes --all-tickers -y 2024
```

**All files:**
```bash
python indicators.py L:\minutes --all-tickers -a
```

## Important Rules

> [!IMPORTANT]
> - You **must** provide a ticker source: `-t`, `--all-tickers`, or `--bulk-file`
> - You **must** provide a file/date mode: `-f`, `-y`, `-a`, or `--bulk-file`
> - `--start-date` and `--end-date` are **filters**, not modes—they must be used with `-a`, `-y`, or `-f`

### Common Mistakes

❌ **Incorrect** (missing mode flag):
```bash
python indicators.py L:\minutes -t AAPL --start-date 2024-01-01 --end-date 2024-01-05
```

✅ **Correct** (includes `-a` mode):
```bash
python indicators.py L:\minutes -t AAPL --start-date 2024-01-01 --end-date 2024-01-05 -a
```

## Output

The script calculates and adds the following columns to your data files:

- **VWAP** – Volume Weighted Average Price
- **EMA_5** – 5-period Exponential Moving Average
- **EMA_9** – 9-period Exponential Moving Average
- **EMA_20** – 20-period Exponential Moving Average
- **EMA_50** – 50-period Exponential Moving Average
- **EMA_200** – 200-period Exponential Moving Average

