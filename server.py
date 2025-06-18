import os
import gzip
import json
import time
import queue
import threading
import sys
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context, redirect, url_for, make_response
import pytz
import statistics
from pathlib import Path
import filelock
from waitress import serve

app = Flask(__name__)

sse_clients = {}
DATA_DIR = None
ONE_MIN_DIR = None
SPECIAL_LINK = None
SERVER_CACHE = {}

file_locks = {}
def get_file_lock(file_path):
    if file_path not in file_locks:
        lock_file_path = f"{file_path}.applock"
        file_locks[file_path] = filelock.FileLock(lock_file_path, timeout=5)
    return file_locks[file_path]


def get_data_directory():
    global DATA_DIR, ONE_MIN_DIR
    
    if DATA_DIR and os.path.exists(DATA_DIR):
        return DATA_DIR
    
    env_dir = os.environ.get('STOCK_DATA_DIR')
    if env_dir and os.path.exists(env_dir):
        DATA_DIR = env_dir
        ONE_MIN_DIR = DATA_DIR
        return DATA_DIR
        
    while True:
        print("Enter the path to your data directory containing CSV.GZ files:")
        print("Example: U:\\1min\\data")
        
        user_dir = input("> ").strip()
        
        if os.path.exists(user_dir):
            DATA_DIR = user_dir
            ONE_MIN_DIR = DATA_DIR
            return DATA_DIR
        else:
            print(f"Error: Directory '{user_dir}' does not exist. Please enter a valid directory path.")

get_data_directory()

def validate_data_exists(ticker, from_date_str, to_date_str):
    print("Validating input...")
    
    all_files = find_csv_gz_files(ONE_MIN_DIR)
    target_file_name = f"{from_date_str}.csv.gz"
    target_file_path = None

    for file_path in all_files:
        if os.path.basename(file_path) == target_file_name:
            target_file_path = file_path
            break
            
    if not target_file_path:
        print(f"VALIDATION FAILED: Data file for from_date '{from_date_str}' not found.")
        return False
        
    try:
        with gzip.open(target_file_path, "rt") as f:
            next(f)
            for line in f:
                fields = line.strip().split(',')
                if fields and fields[0].strip() == ticker:
                    return True
    except Exception as e:
        print(f"VALIDATION ERROR: Could not read file {target_file_path}: {str(e)}")
        return False
        
    return False

def preload_data(ticker, from_date_str, to_date_str):
    print(f"BACKGROUND: Starting data pre-loading for {ticker} from {from_date_str} to {to_date_str}")
    
    start_dt = datetime.strptime(from_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(to_date_str, "%Y-%m-%d")

    files_to_process = []
    for file_path in find_csv_gz_files(ONE_MIN_DIR):
        try:
            file_date = datetime.strptime(os.path.basename(file_path).replace(".csv.gz", ""), "%Y-%m-%d")
            if start_dt <= file_date <= end_dt:
                files_to_process.append(file_path)
        except ValueError:
            continue
            
    if not files_to_process:
        print(f"BACKGROUND: No files found for the specified date range.")
        return

    results = process_files_chronologically(sorted(files_to_process), ticker, '1D', 'background_loader', start_dt, end_dt)
    
    if not results:
        print(f"BACKGROUND: Pre-loading failed. No data was processed for {ticker}.")
        return
        
    combined_data = combine_results_in_order(results)
    
    cache_key = f"{ticker}-{from_date_str}-{to_date_str}-1D"
    SERVER_CACHE[cache_key] = combined_data
    print(f"BACKGROUND: Minute data for {ticker} from {from_date_str} to {to_date_str} has been cached.")


def convert_epoch_to_ny_time(epoch_ns):
    try:
        epoch_seconds = epoch_ns / 1_000_000_000
        utc_time = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        ny_tz = pytz.timezone('America/New_York')
        ny_time = utc_time.astimezone(ny_tz)
        return int(ny_time.timestamp())
    except Exception as e:
        return None

def find_csv_gz_files(directory):
    csv_gz_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv.gz"):
                csv_gz_files.append(os.path.join(root, file))
    return csv_gz_files

def calculate_and_add_vwap(file_path, ticker):
    file_lock = get_file_lock(file_path)
    
    try:
        with file_lock:
            with gzip.open(file_path, "rt") as f:
                lines = f.readlines()

            if not lines:
                return False

            header_line = lines[0].strip()
            header_fields = header_line.split(',')
            
            file_has_vwap_column = 'VWAP' in header_fields
            vwap_index = header_fields.index('VWAP') if file_has_vwap_column else len(header_fields)

            ticker_data_for_calc = []
            lines_to_keep_original = {} 
            
            ticker_has_vwap_data_in_file = False
            for i, line in enumerate(lines[1:]):
                fields = line.strip().split(',')
                if not fields or fields[0] != ticker or len(fields) <= 6:
                    lines_to_keep_original[i + 1] = line
                    continue

                if file_has_vwap_column and vwap_index < len(fields) and fields[vwap_index].strip():
                    ticker_has_vwap_data_in_file = True
                    lines_to_keep_original[i + 1] = line
                else:
                    try:
                        timestamp = int(fields[6])
                        open_price = float(fields[2])
                        close_price = float(fields[3])
                        high_price = float(fields[4])
                        low_price = float(fields[5])
                        volume = float(fields[1])
                        
                        ticker_data_for_calc.append({
                            'line_index': i + 1,
                            'fields': fields,
                            'timestamp': timestamp,
                            'open': open_price,
                            'close': close_price,
                            'high': high_price,
                            'low': low_price,
                            'volume': volume
                        })
                    except (ValueError, IndexError) as e:
                        lines_to_keep_original[i + 1] = line
                        continue
            
            if ticker_has_vwap_data_in_file:
                return True

            if not ticker_data_for_calc:
                return False

            ticker_data_for_calc.sort(key=lambda x: x['timestamp'])
            
            cumulative_tp_volume = 0
            cumulative_volume = 0
            vwap_values = {}
            
            for point in ticker_data_for_calc:
                typical_price = (point['high'] + point['low'] + point['close']) / 3
                cumulative_tp_volume += typical_price * point['volume']
                cumulative_volume += point['volume']
                
                if cumulative_volume > 0:
                    vwap = cumulative_tp_volume / cumulative_volume
                else:
                    vwap = typical_price
                
                vwap_values[point['timestamp']] = vwap

            new_lines_content = []
            if not file_has_vwap_column:
                new_lines_content.append(header_line + ',VWAP\n')
            else:
                new_lines_content.append(header_line + '\n')

            for i, original_line in enumerate(lines[1:]):
                line_index_in_file = i + 1
                
                if line_index_in_file in lines_to_keep_original:
                    new_lines_content.append(lines_to_keep_original[line_index_in_file])
                else:
                    fields = original_line.strip().split(',')
                    try:
                        timestamp = int(fields[6])
                        if timestamp in vwap_values:
                            while len(fields) <= vwap_index:
                                fields.append('')
                            
                            fields[vwap_index] = str(vwap_values[timestamp])
                            new_lines_content.append(','.join(fields) + '\n')
                        else:
                            new_lines_content.append(original_line)
                    except (ValueError, IndexError):
                        new_lines_content.append(original_line)

            with gzip.open(file_path, "wt") as f_out:
                f_out.writelines(new_lines_content)
            
            print(f"SERVER LOG: VWAP calculated and file updated for {ticker} in {os.path.basename(file_path)}")
            return True
        
    except filelock.Timeout:
        print(f"SERVER LOG: File {file_path} is locked by another process. Skipping VWAP calculation for this file.")
        return True
    except Exception as e:
        print(f"SERVER ERROR: Error calculating VWAP for {ticker} in {file_path}: {str(e)}")
        return False
    finally:
        pass


def process_single_file(file_path, ticker):
    print(f"SERVER LOG: Processing file {os.path.basename(file_path)} for ticker {ticker}")
    try:
        vwap_success = calculate_and_add_vwap(file_path, ticker)
        
        if not vwap_success:
            return {"status": "error", "message": f"Failed to ensure VWAP for {ticker} in {file_path}"}
            
        candlestick_data = []
        volume_data = []
        vwap_data = []
        
        with gzip.open(file_path, "rt") as f:
            header = next(f).strip().split(',')
            has_vwap = 'VWAP' in header
            vwap_index = header.index('VWAP') if has_vwap else -1

            for line in f:
                fields = line.strip().split(',')
                if len(fields) < 7:
                    continue

                if fields[0] == ticker:
                    try:
                        window_start_ns = int(fields[6])
                        time_in_seconds = convert_epoch_to_ny_time(window_start_ns)
                        
                        if time_in_seconds is None:
                            continue

                        open_price = float(fields[2])
                        close_price = float(fields[3])
                        high_price = float(fields[4])
                        low_price = float(fields[5])
                        volume_value = float(fields[1])

                        candlestick_data.append({
                            "time": time_in_seconds,
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                        })

                        volume_data.append({
                            "time": time_in_seconds,
                            "value": volume_value,
                            "color": '#26a69a' if close_price >= open_price else '#ef5350',
                        })

                        if has_vwap and vwap_index != -1 and vwap_index < len(fields) and fields[vwap_index]:
                            try:
                                vwap_value = float(fields[vwap_index])
                                vwap_data.append({
                                    "time": time_in_seconds,
                                    "value": vwap_value,
                                    "exactTime": True
                                })
                            except ValueError:
                                vwap_data.append({
                                    "time": time_in_seconds,
                                    "value": None,
                                    "exactTime": True
                                })
                        else:
                             vwap_data.append({
                                "time": time_in_seconds,
                                "value": None,
                                "exactTime": True
                            })

                    except (ValueError, IndexError) as e:
                        pass
        print(f"SERVER LOG: Finished processing file {os.path.basename(file_path)} for ticker {ticker}. Extracted {len(candlestick_data)} points.")
        return {
            "status": "success",
            "candlestick": candlestick_data,
            "volume": volume_data,
            "vwap": vwap_data
        }
        
    except Exception as e:
        print(f"SERVER ERROR: Error extracting data from {os.path.basename(file_path)}: {str(e)}")
        return {"status": "error", "message": str(e)}

def process_files_chronologically(file_paths, ticker, range_type_for_partial_updates, client_id, start_dt, end_dt):
    print(f"SERVER LOG: Starting chronological processing of {len(file_paths)} files for ticker {ticker} and client {client_id}")
    
    sorted_files = sorted(file_paths)
    max_workers = min(os.cpu_count() or 1, len(sorted_files))
    
    results = {}
    successful_files = []
    failed_files = []
    processed_count = 0
    total_count = len(sorted_files)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file_path in sorted_files:
            future = executor.submit(process_single_file, file_path, ticker)
            file_date = os.path.basename(file_path).replace('.csv.gz', '')
            futures.append((future, file_path, file_date))
        
        for future, file_path, file_date in futures:
            try:
                result_for_file = future.result()
                processed_count += 1
                
                if result_for_file["status"] == "success":
                    results[file_date] = result_for_file
                    successful_files.append(file_date)
                else:
                    failed_files.append(file_date)
                
                send_progress_update(client_id, file_date, processed_count, total_count)
                
                if result_for_file["status"] == "success" and client_id != 'background_loader' and range_type_for_partial_updates == '1D':
                    send_partial_data_update(client_id, results, range_type_for_partial_updates, start_dt, end_dt)
                
            except Exception as e:
                processed_count += 1
                failed_files.append(file_date)
                send_sse_message(client_id, 'error', {
                    'file': file_date,
                    'message': str(e)
                })
    
    print(f"SERVER LOG: Sending processing summary to client {client_id}.")
    send_sse_message(client_id, 'processing_summary', {
        'total_files': total_count,
        'successful_files': successful_files,
        'failed_files': failed_files
    })
            
    print(f"SERVER LOG: Chronological processing finished for ticker {ticker}. Successful files: {len(successful_files)}, Failed files: {len(failed_files)}.")
    return results

def combine_results_in_order(results):
    all_candlesticks = []
    all_volumes = []
    all_vwaps = []
    
    dates = sorted(results.keys())
    
    for date in dates:
        if results[date]["status"] == "success":
            all_candlesticks.extend(results[date]["candlestick"])
            all_volumes.extend(results[date]["volume"])
            all_vwaps.extend(results[date]["vwap"])
    
    all_candlesticks.sort(key=lambda x: x["time"])
    all_volumes.sort(key=lambda x: x["time"])
    all_vwaps.sort(key=lambda x: x["time"])
    
    print(f"SERVER LOG: Combined results into {len(all_candlesticks)} total data points.")
    return {
        "candlestick": all_candlesticks,
        "volume": all_volumes,
        "vwap": all_vwaps
    }

def aggregate_data_by_type(data, range_type, data_type, start_date=None, end_date=None):
    if not data:
        return []
        
    aggregated_data = {}
    
    for data_point in data:
        if 'time' not in data_point:
            continue
            
        date = datetime.fromtimestamp(data_point['time'], tz=pytz.timezone('America/New_York'))
        
        if range_type == 'D':
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            key = f"{start_of_day.year}-{start_of_day.month:02d}-{start_of_day.day:02d}"
            time_key = int(start_of_day.timestamp())
        elif range_type == '1W':
            weekday = date.weekday()
            start_of_week = date - timedelta(days=weekday)
            key = f"{start_of_week.year}-W{start_of_week.isocalendar()[1]}"
            time_key = int(start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        elif range_type == '1M':
            start_of_month = date.replace(day=1)
            key = f"{start_of_month.year}-M{start_of_month.month:02d}"
            time_key = int(start_of_month.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        elif range_type == '1Y':
            start_of_year = date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            key = f"{start_of_year.year}"
            time_key = int(start_of_year.timestamp())
        else:
            key = str(data_point['time'])
            time_key = data_point['time']
            
        if key not in aggregated_data:
            aggregated_data[key] = {
                'time': time_key,
                'data_points': [],
                'first_time': data_point['time'],
            }
            
            if data_type == 'candlestick':
                aggregated_data[key].update({
                    'open': data_point.get('open', 0),
                    'high': data_point.get('high', 0),
                    'low': data_point.get('low', float('inf')),
                    'close': data_point.get('close', 0),
                    'volume': 0,
                })
                if aggregated_data[key]['low'] == float('inf') and data_point.get('low') is not None:
                    aggregated_data[key]['low'] = data_point.get('low', 0)
            elif data_type == 'volume':
                aggregated_data[key].update({
                    'value': 0,
                    'up_volume': 0,
                    'down_volume': 0,
                })
            elif data_type == 'vwap':
                aggregated_data[key].update({
                    'value': None,
                    'vwap_values': [],
                    'exactTime': True,
                })
        
        aggregated_data[key]['data_points'].append(data_point)
        
        if data_type == 'candlestick':
            if data_point['time'] < aggregated_data[key]['first_time']:
                aggregated_data[key]['open'] = data_point.get('open', aggregated_data[key]['open'])
                aggregated_data[key]['first_time'] = data_point['time']
                
            aggregated_data[key]['close'] = data_point.get('close', aggregated_data[key]['close'])
            
            if data_point.get('high', 0) > aggregated_data[key]['high']:
                aggregated_data[key]['high'] = data_point.get('high', 0)
            if data_point.get('low', float('inf')) < aggregated_data[key]['low'] and data_point.get('low', 0) >= 0:
                aggregated_data[key]['low'] = data_point.get('low', aggregated_data[key]['low'])
                
            aggregated_data[key]['volume'] += data_point.get('volume', 0)
        elif data_type == 'volume':
            value = data_point.get('value', 0)
            aggregated_data[key]['value'] += value
            
            if data_point.get('color') == '#26a69a':
                aggregated_data[key]['up_volume'] += value
            else:
                aggregated_data[key]['down_volume'] += value
        elif data_type == 'vwap':
            if 'value' in data_point and data_point['value'] is not None:
                aggregated_data[key]['vwap_values'].append(data_point['value'])
    
    result = []
    for key, aggregate in aggregated_data.items():
        if data_type == 'candlestick':
            result.append({
                'time': aggregate['time'],
                'open': aggregate['open'],
                'high': aggregate['high'],
                'low': aggregate['low'] if aggregate['low'] != float('inf') else aggregate['open'],
                'close': aggregate['close'],
                'volume': aggregate['volume']
            })
        elif data_type == 'volume':
            color = '#26a69a' if aggregate['up_volume'] >= aggregate['down_volume'] else '#ef5350'
            result.append({
                'time': aggregate['time'],
                'value': aggregate['value'],
                'color': color
            })
        elif data_type == 'vwap':
            if aggregate['vwap_values']:
                vwap_value = statistics.median(aggregate['vwap_values'])
            else:
                vwap_value = None
                
            result.append({
                'time': aggregate['time'],
                'value': vwap_value,
                'exactTime': True
            })
    
    result.sort(key=lambda x: x['time'])
    return result

def aggregate_data(combined_data, range_type, start_dt, end_dt):
    candlestick_set = [dict(item) for item in combined_data.get("candlestick", [])]
    volume_set = [dict(item) for item in combined_data.get("volume", [])]
    vwap_set = [dict(item) for item in combined_data.get("vwap", [])] 
    
    candlestick_data = aggregate_data_by_type(candlestick_set, range_type, 'candlestick', start_dt, end_dt)
    volume_data = aggregate_data_by_type(volume_set, range_type, 'volume', start_dt, end_dt)
    vwap_data = aggregate_data_by_type(vwap_set, range_type, 'vwap', start_dt, end_dt)
    
    if vwap_data:
        vwap_map = {vwap['time']: vwap for vwap in vwap_data}
        aligned_vwap = []
        
        for candle in candlestick_data:
            if candle['time'] in vwap_map:
                aligned_vwap.append(vwap_map[candle['time']])
            else:
                aligned_vwap.append({
                    'time': candle['time'],
                    'value': None,
                    'exactTime': True
                })
        vwap_data = aligned_vwap
    
    print(f"SERVER LOG: Aggregated data to {range_type} range. Resulting candlesticks: {len(candlestick_data)}.")
    return {
        "candlestick": candlestick_data,
        "volume": volume_data,
        "vwap": vwap_data
    }

@app.route('/sse-connect')
def sse_connect():
    def event_stream():
        client_queue = queue.Queue()
        client_id = str(time.time())
        sse_clients[client_id] = client_queue
        print(f"SERVER LOG: SSE Client Connected: {client_id}")
        
        yield f"data: {json.dumps({'client_id': client_id, 'type': 'connected'})}\n\n"
        
        try:
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield f"data: {json.dumps(message)}\n\n"
                    
                    if message.get('type') == 'complete':
                        print(f"SERVER LOG: SSE Processing Complete for client {client_id}.")
                        break
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            if client_id in sse_clients:
                del sse_clients[client_id]
                print(f"SERVER LOG: SSE Client Disconnected: {client_id}")
    
    return Response(stream_with_context(event_stream()), 
                   mimetype='text/event-stream')

def send_sse_message(client_id, message_type, data):
    if client_id and client_id != 'background_loader' and client_id in sse_clients:
        sse_clients[client_id].put({
            'type': message_type,
            'timestamp': time.time(),
            'data': data
        })

def send_progress_update(client_id, file_date, completed, total):
    send_sse_message(client_id, 'progress', {
        'file_date': file_date,
        'completed': completed,
        'total': total,
        'percentage': (completed / total) * 100
    })

def send_partial_data_update(client_id, results, range_type=None, start_dt=None, end_dt=None):
    if range_type == '1D':
        combined_data = combine_results_in_order(results)
        send_sse_message(client_id, 'partial_data', combined_data)
        print(f"SERVER LOG: Sent partial data update (1D range, {len(combined_data['candlestick'])} points) to client {client_id}.")


def process_files_and_send_updates(files, ticker, requested_range_type, client_id, start_dt, end_dt):
    print(f"SERVER LOG: Starting background processing thread for client {client_id}, ticker {ticker}, requested range {requested_range_type}.")
    try:
        sorted_files = sorted(files)
        
        results = process_files_chronologically(sorted_files, ticker, requested_range_type, client_id, start_dt, end_dt)
        
        if not results:
            send_sse_message(client_id, 'error', {
                'message': "No data could be processed successfully for the selected date range"
            })
            print(f"SERVER LOG: Background processing failed for client {client_id}: No data processed.")
            return
        
        combined_data = combine_results_in_order(results)
        
        if not combined_data["candlestick"]:
            send_sse_message(client_id, 'error', {
                'message': "No valid price data found for the selected ticker and date range"
            })
            print(f"SERVER LOG: Background processing failed for client {client_id}: No valid price data.")
            return
        
        result_dates = sorted(list(results.keys()))
        
        final_data_to_send = {}
        if requested_range_type != '1D':
            final_data_to_send = aggregate_data(combined_data, requested_range_type, start_dt, end_dt)
        else:
            final_data_to_send = combined_data
        
        final_data_to_send["processed_dates"] = result_dates
        send_sse_message(client_id, 'complete', final_data_to_send)
        print(f"SERVER LOG: Final data (range: {requested_range_type}, points: {len(final_data_to_send['candlestick'])}) sent to client {client_id}.")
            
    except Exception as e:
        print(f"SERVER ERROR: Unhandled exception in background processing thread for client {client_id}: {str(e)}")
        send_sse_message(client_id, 'error', {
            'message': f"An unexpected server error occurred: {str(e)}"
        })

@app.route("/")
def index():
    if SPECIAL_LINK:
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Chart Link</title>
            <style>
                body {{ font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f4f4f4; }}
                .container {{ text-align: center; padding: 20px; border-radius: 8px; background-color: white; box-shadow: 0 44px 8px rgba(0,0,0,0.1); }}
                h2 {{ color: #333; }}
                p {{ color: #666; }}
                a {{ font-size: 1.2em; color: #007bff; text-decoration: none; word-break: break-all;}}
                a:hover {{ text-decoration: underline; }}
                .status {{ margin-top: 15px; font-style: italic; color: #555; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>A specific chart has been requested.</h2>
                <p>The server is preparing your data in the background.</p>
                <p>Click the link below to view the chart:</p>
                <a href="{SPECIAL_LINK}">{SPECIAL_LINK}</a>
                <p class="status">The chart will load automatically when the data is ready.</p>
            </div>
        </body>
        </html>
        """
    return redirect(url_for('chart_page'))

@app.route("/chart")
def chart_page():
    response = make_response(send_from_directory(".", "index.html"))
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response

@app.route("/one_minute_data_range", methods=["GET"])
def one_minute_data_range():
    request_start_time = time.time()
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        ticker = request.args.get("ticker", "MSFT")
        requested_range_type = request.args.get("range", "1D") 
        client_id = request.args.get("client_id")

        if not start_date:
            return jsonify({"error": "start_date is required"}), 400
            
        if not end_date:
            end_date = start_date

        cache_key_requested = f"{ticker}-{start_date}-{end_date}-{requested_range_type}"
        
        # 1. Check if the EXACT requested range is in cache
        if cache_key_requested in SERVER_CACHE:
            print(f"SERVER LOG: Serving {cache_key_requested} from CACHE. (Points: {len(SERVER_CACHE[cache_key_requested]['candlestick'])}). Request served in {time.time() - request_start_time:.2f} seconds.")
            return jsonify(SERVER_CACHE[cache_key_requested])

        # 2. If requested range not in cache, and it's an aggregated range,
        #    check if the base minute data (1D) is in cache.
        cache_key_1D_base = f"{ticker}-{start_date}-{end_date}-1D"
        if requested_range_type != '1D' and cache_key_1D_base in SERVER_CACHE:
            print(f"SERVER LOG: {cache_key_requested} not in cache. Aggregating from cached 1D base data.")
            cached_minute_data = SERVER_CACHE[cache_key_1D_base]
            
            start_dt_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt_obj = datetime.strptime(end_date, "%Y-%m-%d")
            
            aggregated_data = aggregate_data(cached_minute_data, requested_range_type, start_dt_obj, end_dt_obj)
            
            # Cache the newly aggregated data for future direct requests
            SERVER_CACHE[cache_key_requested] = aggregated_data
            print(f"SERVER LOG: Aggregated and cached {requested_range_type} data from {cache_key_1D_base}. Served in {time.time() - request_start_time:.2f} seconds.")
            return jsonify(aggregated_data)

        # 3. If neither the requested aggregated data nor the base minute data is in cache,
        #    then proceed with file processing (asynchronously via SSE or synchronously).
        print(f"SERVER LOG: Data for {cache_key_requested} not in cache, and 1D base data not available. Proceeding with file processing.")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
        files_to_process = []
        
        for file_path in csv_gz_files:
            try:
                file_name = os.path.basename(file_path)
                file_date_str = file_name.replace(".csv.gz", "")
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                
                if start_dt <= file_date <= end_dt:
                    files_to_process.append(file_path)
            except Exception as e:
                print(f"SERVER ERROR: Error parsing file date {file_path}: {str(e)}")
        
        files_to_process.sort()
        
        if not files_to_process:
            return jsonify({"error": "No data files found for the specified date range."}), 404

        if client_id in sse_clients: 
            thread = threading.Thread(
                target=process_files_and_send_updates,
                args=(files_to_process, ticker, requested_range_type, client_id, start_dt, end_dt)
            )
            thread.daemon = True
            thread.start()
            print(f"SERVER LOG: Async processing started for {len(files_to_process)} files (client: {client_id}, range: {requested_range_type}). Response time: {time.time() - request_start_time:.2f} seconds (non-blocking).")
            return jsonify({
                "status": "processing_started",
                "files_count": len(files_to_process)
            })
        else:
            print(f"SERVER LOG: Synchronous processing initiated for {len(files_to_process)} files (range: {requested_range_type}). This will block until complete.")
            raw_minute_results = process_files_chronologically(files_to_process, ticker, '1D', 'sync_request_no_sse', start_dt, end_dt)
            combined_minute_data = combine_results_in_order(raw_minute_results)

            final_data = {}
            if requested_range_type != '1D':
                final_data = aggregate_data(combined_minute_data, requested_range_type, start_dt, end_dt)
            else:
                final_data = combined_minute_data
            
            SERVER_CACHE[cache_key_requested] = final_data # Cache the result for the requested key
            print(f"SERVER LOG: Synchronous processing complete for {cache_key_requested}. Total time: {time.time() - request_start_time:.2f} seconds.")
            return jsonify(final_data)
            
    except Exception as e:
        print(f"SERVER ERROR: Unhandled exception in one_minute_data_range route: {str(e)}")
        return jsonify({"error": f"Failed to load data: {str(e)}"}), 500

@app.route("/available_dates", methods=["GET"])
def available_dates():
    request_start_time = time.time()
    try:
        csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
        dates = [os.path.basename(f).replace(".csv.gz", "") for f in csv_gz_files]
        print(f"SERVER LOG: Sent {len(dates)} available dates to frontend. Response time: {time.time() - request_start_time:.2f} seconds.")
        return jsonify({"dates": sorted(dates)})
    except Exception as e:
        print(f"SERVER ERROR: Error fetching available dates: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/available_tickers", methods=["GET"])
def available_tickers():
    request_start_time = time.time()
    date = request.args.get("date", "2016-01-04")
    csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
    file_path = None
    for file in csv_gz_files:
        if date in file:
            file_path = file
            break
    if not file_path:
        print(f"SERVER LOG: No data file found for date: {date} for ticker lookup.")
        return jsonify({"error": f"No data found for {date}"}), 404
    tickers = set()
    try:
        with gzip.open(file_path, "rt") as f:
            next(f)
            for line in f:
                fields = line.strip().split(',')
                if fields:
                    tickers.add(fields[0])
        print(f"SERVER LOG: Sent {len(tickers)} available tickers for date {date} to frontend. Response time: {time.time() - request_start_time:.2f} seconds.")
        return jsonify({"tickers": sorted(list(tickers))})
    except Exception as e:
        print(f"SERVER ERROR: Error reading tickers from {file_path}: {str(e)}")
        return jsonify({"error": str(e)}), 500

def print_startup_message(port):
    global SPECIAL_LINK
    print("=" * 60)
    print("=" * 60)
    print(f"Working directory: {os.getcwd()}")
    print(f"Data directory: {DATA_DIR}")
    
    if not os.path.exists(DATA_DIR):
        print(f"WARNING: Data directory {DATA_DIR} does not exist!")
    else:
        csv_gz_count = len(find_csv_gz_files(DATA_DIR))
        print(f"Found {csv_gz_count} CSV.GZ files in the data directory")
        
    if len(sys.argv) >= 4:
        ticker = sys.argv[1]
        from_date = sys.argv[2]
        to_date = sys.argv[3]
        
        range_type_map = {
            "minutes": "1D",
            "daily": "D",
            "d": "D", 
            "w": "1W", 
            "1w": "1W",
            "week": "1W", 
            "month": "1M",
            "m": "1M", 
            "1m": "1M",
            "year": "1Y",
            "y": "1Y", 
            "1y": "1Y"
        }
        
        cli_range_arg = sys.argv[4].lower() if len(sys.argv) == 5 else "minutes" 
        requested_display_range = range_type_map.get(cli_range_arg, "1D") 
        
        if validate_data_exists(ticker, from_date, to_date):
            SPECIAL_LINK = (
                f"http://localhost:{port}/chart"
                f"?ticker={ticker}"
                f"&from={from_date}"
                f"&to={to_date}"
                f"&range={requested_display_range}"
            )
            print("-" * 60)
            print("Chart pre-configured with command-line arguments.")
            print(f"Link: {SPECIAL_LINK}") 
            
            preload_thread = threading.Thread(target=preload_data, args=(ticker, from_date, to_date)) 
            preload_thread.daemon = True
            preload_thread.start() 
            print("Minute data is being pre-loaded and cached in the background...") 
            print("-" * 60)
        else:
            print("-" * 60)
            print("Could not validate data for the provided arguments.")
            print("Starting server in default mode.")
            print("-" * 60)

    if not SPECIAL_LINK:
         print(f"Server running at http://localhost:{port}/chart")
    
    print("=" * 60)

if __name__ == "__main__":
    port = 8000
    print_startup_message(port)
    
    print(f"Running server on port {port}")
    try:
        serve(app, host="0.0.0.0", port=port)
    except Exception as e:
        print(f"SERVER CRITICAL ERROR: Waitress server failed to start or crashed: {str(e)}")
        sys.exit(1)

