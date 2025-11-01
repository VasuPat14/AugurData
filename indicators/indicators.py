import pandas as pd
import gzip
import os
import argparse
import sys
import re
from datetime import datetime, date
import tempfile
import shutil
from pathlib import Path
from collections import defaultdict
import json

# -------------------------
# Configuration
# -------------------------
INDICATORS = {
    'VWAP': {'type': 'vwap'},
    '5ema': {'type': 'ema', 'period': 5},
    '9ema': {'type': 'ema', 'period': 9},
    '20ema': {'type': 'ema', 'period': 20},
    '50ema': {'type': 'ema', 'period': 50},
    '200ema': {'type': 'ema', 'period': 200},
}

REQUIRED_COLUMNS = {
    "ticker": "ticker",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "timestamp": "window_start"
}

FILENAME_DATE_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).*\.csv\.gz$")

# -------------------------
# Core Indicator Calculations
# -------------------------
def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculate Volume Weighted Average Price."""
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    tp_volume = typical_price * df['volume']
    cumulative_tp_volume = tp_volume.cumsum()
    cumulative_volume = df['volume'].cumsum()
    
    vwap = cumulative_tp_volume / cumulative_volume.replace(0, float('nan'))
    return vwap


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def calculate_indicators_for_ticker(ticker_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all indicators for a single ticker's dataframe."""
    result_df = ticker_df.copy()
    
    required_cols = ['high', 'low', 'close', 'volume']
    if not all(col in result_df.columns for col in required_cols):
        raise ValueError(f"Missing required columns for calculation: {required_cols}")
    
    if len(result_df) == 0:
        # This is not an error, just an empty set for this ticker on this day.
        return result_df
    
    try:
        result_df['VWAP'] = calculate_vwap(result_df)
        
        for indicator_name, params in INDICATORS.items():
            if params['type'] == 'ema':
                result_df[indicator_name] = calculate_ema(result_df['close'], params['period'])
        
        return result_df
    
    except Exception as e:
        print(f"  ERROR: Failed during indicator calculation: {e}", file=sys.stderr)
        raise

# -------------------------
# File Writing
# -------------------------
def write_file_standard(file_path: str, df: pd.DataFrame) -> str:
    """Standard full file write with consistent date formatting."""
    temp_file_path = None
    file_basename = os.path.basename(file_path)
    original_csv_name = file_basename.replace('.gz', '')
    
    try:
        temp_dir = os.path.dirname(file_path) or '.'
        # Create a temporary file to write to, ensuring atomicity
        with tempfile.NamedTemporaryFile(
            mode='wb', dir=temp_dir, prefix='.tmp_', suffix='.csv.gz', delete=False
        ) as temp_file:
            temp_file_path = temp_file.name

        print(f"  Writing updated data to temporary file...")
        # Ensure consistent date format in the output CSV
        csv_buffer = df.to_csv(index=False, lineterminator='\n', date_format='%Y-%m-%d %H:%M:%S')
        
        with gzip.GzipFile(filename=original_csv_name, mode='wb', fileobj=open(temp_file_path, 'wb')) as gz:
            gz.write(csv_buffer.encode('utf-8'))
        
        return temp_file_path
    
    except Exception as e:
        # Clean up the temporary file on failure
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise

# -------------------------
# File Processing
# -------------------------
def needs_update(df: pd.DataFrame, ticker: str, indicator_columns: list) -> bool:
    """Check if a specific ticker in the dataframe needs indicator calculations."""
    # If any indicator columns are missing entirely, an update is needed.
    if any(col not in df.columns for col in indicator_columns):
        return True
    
    ticker_mask = df[REQUIRED_COLUMNS['ticker']] == ticker
    # If the ticker has no data, no update is needed.
    if not ticker_mask.any():
        return False
        
    # Check if all indicator values for this ticker are null/empty.
    return df.loc[ticker_mask, indicator_columns].isnull().all().all()


def process_file(file_path: str, tickers_to_process: list | None) -> bool:
    """Process a single data file, calculate indicators, and overwrite atomically."""
    temp_file_path = None
    file_basename = os.path.basename(file_path)
    
    try:
        print(f"  Reading {file_basename}...")
        
        # Use chunked reading for very large files to manage memory
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                df = pd.read_csv(f, low_memory=False)
        except (FileNotFoundError, Exception) as e:
            print(f"  ERROR: Failed to read file {file_path}: {e}", file=sys.stderr)
            return False
        
        total_rows = len(df)
        print(f"  Total rows read: {total_rows:,}")
        
        # Validate that the CSV has the necessary columns to proceed
        missing_cols = [col for col in REQUIRED_COLUMNS.values() if col not in df.columns]
        if missing_cols:
            print(f"  ERROR: Missing required columns in source file: {missing_cols}", file=sys.stderr)
            return False
        
        # Determine which tickers to focus on for this file
        available_tickers = df[REQUIRED_COLUMNS['ticker']].unique()
        if tickers_to_process is None: # --all-tickers mode
            tickers_to_process = available_tickers.tolist()
        else: # Specific tickers from CLI or bulk file
            tickers_to_process = [t for t in tickers_to_process if t in available_tickers]
        
        if not tickers_to_process:
            print(f"  INFO: No matching tickers to process were found in {file_basename}.")
            return True
        
        # Identify which of the targeted tickers actually need calculations
        indicator_columns = list(INDICATORS.keys())
        tickers_needing_update = [t for t in tickers_to_process if needs_update(df, t, indicator_columns)]
        
        if not tickers_needing_update:
            print(f"  INFO: All requested tickers already have up-to-date indicators.")
            return True
        
        print(f"  Calculating indicators for {len(tickers_needing_update)} ticker(s): {', '.join(sorted(tickers_needing_update))}")
        
        # Process each required ticker and collect the results
        processed_dfs = []
        for ticker in tickers_needing_update:
            ticker_mask = df[REQUIRED_COLUMNS['ticker']] == ticker
            ticker_with_indicators = calculate_indicators_for_ticker(df.loc[ticker_mask])
            processed_dfs.append(ticker_with_indicators)
        
        # Update the main dataframe with the new indicator data
        if processed_dfs:
            processed_combined = pd.concat(processed_dfs, ignore_index=False)
            # Add new indicator columns to the main dataframe if they don't exist
            for col in indicator_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            # Update the specific rows with the newly calculated values
            df.update(processed_combined)

        # Ensure a consistent column order in the output file
        original_columns = [col for col in df.columns if col not in indicator_columns]
        final_column_order = original_columns + [col for col in indicator_columns if col in df.columns]
        df = df[final_column_order]
        
        # Write the modified dataframe to a new temporary file
        temp_file_path = write_file_standard(file_path, df)
        
        # CRITICAL: Validate the temporary file before replacing the original
        print(f"  Validating written file...")
        with gzip.open(temp_file_path, 'rt', encoding='utf-8') as f:
            validation_df = pd.read_csv(f, low_memory=False, nrows=10)
            if list(validation_df.columns) != list(df.columns):
                raise ValueError("Validation failed: Column mismatch in the written file!")
        
        # Atomic Replace: backup original, move new file, then delete backup
        backup_path = file_path + '.backup'
        try:
            shutil.copy2(file_path, backup_path)
            shutil.move(temp_file_path, file_path)
            os.remove(backup_path)
            print(f"  ✓ Successfully updated {file_basename}")
            return True
        except Exception as e:
            print(f"  ERROR: Failed to replace original file. Restoring from backup. Details: {e}", file=sys.stderr)
            if os.path.exists(backup_path):
                shutil.copy2(backup_path, file_path) # Restore
            return False
    
    except Exception as e:
        print(f"  FATAL ERROR processing {file_basename}: {e}", file=sys.stderr)
        return False
    finally:
        # Final cleanup of any lingering temporary files
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# -------------------------
# File Discovery & Planning
# -------------------------
def parse_date_from_filename(filename: str) -> date | None:
    """Extract a date object from a filename like '2024-01-02.csv.gz'."""
    match = FILENAME_DATE_RE.match(filename)
    if match:
        try:
            return date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
        except (ValueError, TypeError):
            return None
    return None

def find_files_to_process(args: argparse.Namespace) -> list[str]:
    """Find all data files that match the manual CLI criteria."""
    directory = Path(args.directory)
    if not directory.is_dir():
        raise FileNotFoundError(f"Directory not found: '{args.directory}'")
    
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    
    files = []
    if args.file:
        file_path = directory / args.file
        # Check for year/month subdirectories like /2024/01/2024-01-02.csv.gz
        if not file_path.exists():
            match = re.match(r'^(\d{4})-(\d{2})-', args.file)
            if match:
                year, month = match.groups()
                file_path = directory / year / month / args.file
        if not file_path.exists():
            raise FileNotFoundError(f"Specified file not found: {args.file}")
        files.append(str(file_path))
    else:
        # Build a prefix to speed up file search
        prefix = f"{args.year}-{str(args.month).zfill(2)}-" if args.year and args.month else (f"{args.year}-" if args.year else "")
        for file_path in directory.rglob("*.csv.gz"):
            if prefix and not file_path.name.startswith(prefix):
                continue
            
            file_date = parse_date_from_filename(file_path.name)
            # Check if the file's date falls within the requested range
            in_range = not (start_date and file_date < start_date) and \
                       not (end_date and file_date > end_date)
            if in_range:
                files.append(str(file_path))
    
    return sorted(files)

def build_work_map_from_bulk_file(bulk_file_path: str, directory: str) -> dict:
    """
    Reads the simplified bulk JSON file and creates an efficient processing plan.
    Expected JSON format: ["YYYY-MM-DD YYYY-MM-DD TICKER", ...]
    """
    print(f"Reading bulk input file: {bulk_file_path}")
    try:
        with open(bulk_file_path, 'r') as f:
            bulk_requests = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Bulk input file not found: {bulk_file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in {bulk_file_path}. Is it a valid list of strings?")

    if not isinstance(bulk_requests, list) or not all(isinstance(i, str) for i in bulk_requests):
        raise TypeError("Bulk input JSON must be a list of strings (e.g., [\"...\"])")

    print("Building efficient work plan...")
    file_to_tickers_map = defaultdict(set)
    
    # Pre-process requests from the JSON file for faster matching
    processed_requests = []
    for i, line in enumerate(bulk_requests):
        parts = line.split()
        if len(parts) != 3:
            raise ValueError(f"Line {i+1} in JSON has incorrect format. Expected 'from to ticker'.")
        from_str, to_str, ticker = parts
        processed_requests.append({
            'from_date': datetime.strptime(from_str, "%Y-%m-%d").date(),
            'to_date': datetime.strptime(to_str, "%Y-%m-%d").date(),
            'ticker': ticker.upper()
        })

    # Iterate through all data files and match them against our pre-processed requests
    for data_file in Path(directory).rglob("*.csv.gz"):
        file_date = parse_date_from_filename(data_file.name)
        if not file_date:
            continue
        for req in processed_requests:
            if req['from_date'] <= file_date <= req['to_date']:
                file_to_tickers_map[str(data_file)].add(req['ticker'])

    return dict(file_to_tickers_map)

# -------------------------
# Command-Line Interface (CLI)
# -------------------------
def main():
    """Main function to parse arguments and orchestrate the processing."""
    parser = argparse.ArgumentParser(
        description="Calculate technical indicators for stock data and update files in place.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process AAPL for a single day's file
  python indicators.py L:\\minutes -t AAPL -f 2024-01-02.csv.gz
  
  # Process all tickers for an entire year
  python indicators.py L:\\minutes --all-tickers -y 2024
  
  # Process multiple tickers for a specific month
  python indicators.py L:\\minutes -t TSLA NVDA -y 2024 -m 3
  
  # Perform a large batch job using a simple JSON input file
  python indicators.py L:\\minutes --bulk-file C:\\data\\requests.json
        """
    )
    
    parser.add_argument("directory", help="Root directory containing the .csv.gz data files")
    
    # Group for specifying which tickers to process
    ticker_group = parser.add_mutually_exclusive_group()
    ticker_group.add_argument("-t", "--ticker", nargs='+', help="One or more specific ticker symbols")
    ticker_group.add_argument("--all-tickers", action='store_true', help="Process all tickers found in each file")
    
    # Group for specifying which files to process (mutually exclusive modes)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-f", "--file", help="A single data file name")
    mode.add_argument("-y", "--year", help="A specific year (e.g., 2024)")
    mode.add_argument("-a", "--all", action='store_true', help="All .csv.gz files in the directory")
    mode.add_argument("--bulk-file", help="Path to a JSON file for bulk processing")
    
    # Optional date filters
    parser.add_argument("-m", "--month", help="A specific month (1-12). Must be used with --year")
    parser.add_argument("--start-date", help="Start date for a custom range (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for a custom range (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Validate argument combinations
    if args.month and not args.year:
        parser.error("--month requires --year")
    
    if not args.bulk_file and not args.ticker and not args.all_tickers:
        parser.error("A ticker source is required. Use --ticker, --all-tickers, or --bulk-file.")

    try:
        print("\n" + "="*70 + "\nTECHNICAL INDICATORS CALCULATOR - OPTIMIZED\n" + "="*70)
        
        work_map = {}
        # Determine the list of files to process based on the mode
        if args.bulk_file:
            work_map = build_work_map_from_bulk_file(args.bulk_file, args.directory)
            files_to_process = sorted(work_map.keys())
            if not files_to_process:
                print("No data files found for the date ranges specified in the bulk file.")
                return 0
        else: # Manual mode
            files_to_process = find_files_to_process(args)
            if not files_to_process:
                print("No files found matching the specified criteria.")
                return 0

        print(f"\nFound {len(files_to_process)} file(s) to process.\n")
        
        # Execute the processing loop
        success_count, failure_count = 0, 0
        for i, file_path in enumerate(files_to_process, 1):
            print(f"[{i}/{len(files_to_process)}] Processing: {os.path.basename(file_path)}")
            
            # Get the list of tickers for the current file
            if args.bulk_file:
                tickers = list(work_map.get(file_path, []))
            elif args.ticker:
                tickers = [t.upper() for t in args.ticker]
            else: # --all-tickers
                tickers = None
            
            if process_file(file_path, tickers):
                success_count += 1
            else:
                failure_count += 1
            print("-" * 40)
        
        # Print final summary
        print("="*70 + f"\nSUMMARY: {success_count} succeeded, {failure_count} failed\n" + "="*70)
        return 0 if failure_count == 0 else 1
    
    except (ValueError, FileNotFoundError, TypeError) as e:
        print(f"\nERROR: A configuration or file issue occurred: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\n\nProcessing interrupted by user.", file=sys.stderr)
        return 1
    except Exception as e:
        import traceback
        print(f"\nUNEXPECTED CRITICAL ERROR: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())