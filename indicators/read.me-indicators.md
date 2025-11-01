#!/bin/bash

# -----------------------------------------------------------------------------
# HELP DOCUMENTATION
# -----------------------------------------------------------------------------
#
# ## Indicator Calculation Script (`indicators.py`)
#
# ### Overview
#
# This is a high-performance command-line utility to calculate technical
# indicators (VWAP, 5/9/20/50/200 EMAs) for stock data.
#
# It reads your existing gzipped CSV files (`.csv.gz`), calculates the new
# indicator values, and then atomically overwrites the original files
# to include the new data as columns.
#
# ### Key Features
#
# * **In-Place Updates:** Modifies your data files directly, adding new
#     columns for each indicator.
# * **Atomic Writes:** Uses a temporary file and replace-on-success strategy
#     to prevent data corruption if the script is interrupted.
# * **Two Modes:**
#     1.  **Bulk Mode:** The most efficient way. Processes a large JSON list
#         of jobs, reading each data file only once.
#     2.  **Manual Mode:** Allows you to specify tickers and date ranges
#         directly in the terminal for one-off tasks.
# * **Resumable:** The script is smart enough to skip tickers that already
#     have indicator data, so you can re-run it on the same files without
#     re-calculating everything.
#
# ### Command-Line Arguments
#
# | Argument | Description | Example |
# | :--- | :--- | :--- |
# | `directory` | **(Positional, Required)** The root directory containing your `.csv.gz` data files. | `L:\minutes` |
# | **Ticker Source** | **(Required: Pick One)** | |
# | `-t`, `--ticker` | Specify one or more ticker symbols to process. | `-t AAPL MSFT` |
# | `--all-tickers` | Process every ticker found within the target files. | `--all-tickers` |
# | **File/Date Mode** | **(Required: Pick One)** | |
# | `--bulk-file` | Path to a JSON input file for bulk processing. | `--bulk-file C:\data\jobs.json` |
# | `-f`, `--file` | Process a single data file name. | `-f 2024-01-02.csv.gz` |
# | `-y`, `--year` | Process all files within a specific year. | `-y 2024` |
# | `-a`, `--all` | Process all `.csv.gz` files found in the directory. | `-a` |
# | **Optional Filters** | | |
# | `-m`, `--month` | A specific month (1-12). **Must be used with `-y`**. | `-y 2024 -m 3` |
# | `--start-date` | The start of a custom date range (YYYY-MM-DD). | `--start-date 2024-01-15` |
# | `--end-date` | The end of a custom date range (YYYY-MM-DD). | `--end-date 2024-01-31` |
#
# ### 1. Bulk Mode Usage (Recommended)
#
# This is the most efficient method for processing many tickers across
# different date ranges.
#
# #### Input File Structure
#
# The script requires a JSON file that is a **list of simple strings**.
# Each string must be in the exact format: `"YYYY-MM-DD YYYY-MM-DD TICKER"`
#
# **Example `requests.json` file:**
# ```json
# [
#   "2024-01-02 2024-01-05 AAPL",
#   "2024-01-03 2024-01-03 MSFT",
#   "2024-01-08 2024-01-10 GOOGL",
#   "2024-02-01 2024-02-28 TSLA"
# ]
# ```
# * This will process `AAPL` from Jan 2 to Jan 5, `MSFT` for only Jan 3, etc.
#
# **Command:**
# ```bash
# python indicators.py L:\minutes --bulk-file C:\data\requests.json
# ```
#
# ---
#
# ### 2. Manual Mode Usage Examples
#
# These commands are for one-off tasks.
#
# #### Processing Specific Tickers (`-t`)
#
# * **For a Single File:**
#     ```bash
#     python indicators.py L:\minutes -t AAPL MSFT -f 2024-01-02.csv.gz
#     ```
#
# * **For an Entire Year:**
#     ```bash
#     python indicators.py L:\minutes -t AAPL -y 2024
#     ```
#
# * **For a Specific Month:**
#     ```bash
#     python indicators.py L:\minutes -t TSLA -y 2024 -m 2
#     ```
#
# * **For a Custom Date Range (using `-a`):**
#     ```bash
#     python indicators.py L:\minutes -t TSLA AAPL MSFT --start-date 2024-02-01 --end-date 2024-02-05 -a
#     ```
#
# * **For a Custom Date Range (within a specific year):**
#     ```bash
#     python indicators.py L:\minutes -t NVDA -y 2024 --start-date 2024-03-01 --end-date 2024-03-05
#     ```
#
# #### Processing All Tickers (`--all-tickers`)
#
# * **For a Single File:**
#     ```bash
#     python indicators.py L:\minutes --all-tickers -f 2024-01-03.csv.gz
#     ```
#
# * **For an Entire Year:**
#     ```bash
#     python indicators.py L:\minutes --all-tickers -y 2024
#     ```
#
# * **For All Files in the Directory:**
#     ```bash
#     python indicators.py L:\minutes --all-tickers -a
#     ```
#
# ---
#
# ### ❗️ Important Usage Rules
#
# 1.  **You MUST provide a ticker source.** You must use *one* of:
#     `-t ...`, `--all-tickers`, or `--bulk-file`.
# 2.  **You MUST provide a file/date mode.** You must use *one* of:
#     `-f ...`, `-y ...`, `-a`, or `--bulk-file`.
# 3.  **Date filters are not modes.** The arguments `--start-date` and
#     `--end-date` are *optional filters*. They do not replace the required
#     mode flag (`-f`, `-y`, or `-a`).
#
# > **Incorrect Command (will fail):**
# > `python indicators.py L:\minutes -t AAPL --start-date 2024-01-01 --end-date 2024-01-05`
# >
# > **Correct Command (adds `-a` to search all files):**
# > `python indicators.py L:\minutes -t AAPL --start-date 2024-01-01 --end-date 2024-01-05 -a`
#
# -----------------------------------------------------------------------------

# Example of how to print this help text from a bash script
# (This is just a conceptual example, the main logic is in Python)
function show_help() {
    # Using 'cat' with a 'here document' (EOF) to print the help text
    cat << 'EOF'
## Indicator Calculation Script (`indicators.py`)

### Overview

This is a high-performance command-line utility to calculate technical...
(etc.)
...
(The rest of the help text goes here)
...
Correct Command (adds `-a` to search all files):
`python indicators.py L:\minutes -t AAPL --start-date 2024-01-01 --end-date 2024-01-05 -a`
EOF
}

# Example of checking for a help flag
# if [[ "$1" == "--help" || "$1" == "-h" ]]; then
#     show_help
#     exit 0
# fi

# The actual script logic would go here
# echo "Running main script logic..."