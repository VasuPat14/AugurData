import os
import gzip
import json
import time
import queue
import threading
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context, redirect, url_for, make_response
import pytz
import statistics
import filelock

app = Flask(__name__)

sse_clients = {}
sse_clients_lock = threading.Lock()
DATA_DIR = None
ONE_MIN_DIR = None
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
        print("Example: L:\\minutes")
        
        user_dir = input("> ").strip()
        
        if os.path.exists(user_dir):
            DATA_DIR = user_dir
            ONE_MIN_DIR = DATA_DIR
            return DATA_DIR
        else:
            print(f"Error: Directory '{user_dir}' does not exist. Please enter a valid directory path.")

get_data_directory()

def convert_epoch_to_ny_time(epoch_ns):
    try:
        epoch_seconds = epoch_ns / 1_000_000_000
        utc_time = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        ny_tz = pytz.timezone('America/New_York')
        ny_time = utc_time.astimezone(ny_tz)
        return int(ny_time.timestamp())
    except Exception:
        return None

def find_csv_gz_files(directory):
    csv_gz_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv.gz"):
                csv_gz_files.append(os.path.join(root, file))
    return csv_gz_files

def process_single_file(file_path, ticker, include_indicators):
    print(f"SERVER LOG: Processing {os.path.basename(file_path)} for {ticker}")
    try:
        candlestick_data = []
        volume_data = []
        
        with gzip.open(file_path, "rt") as f:
            header = next(f).strip().split(',')
            
            # Dynamically identify indicator columns (any column after the 7th position)
            indicator_cols = {header[i]: i for i in range(7, len(header))}
            indicator_data = {name: [] for name in indicator_cols}

            for line in f:
                fields = line.strip().split(',')
                if len(fields) < 7 or fields[0] != ticker:
                    continue

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

                    # Extract all available indicator/extra columns
                    for name, idx in indicator_cols.items():
                        value = None
                        if idx < len(fields) and fields[idx]:
                            try:
                                value = float(fields[idx])
                            except (ValueError, IndexError):
                                value = None
                        
                        indicator_data[name].append({
                            "time": time_in_seconds,
                            "value": value
                        })

                except (ValueError, IndexError):
                    continue
        
        result = {
            "status": "success",
            "candlestick": candlestick_data,
            "volume": volume_data,
            **indicator_data # Unpack all dynamic columns into the result
        }
        
        print(f"SERVER LOG: Extracted {len(candlestick_data)} points from {os.path.basename(file_path)}")
        return result
        
    except Exception as e:
        print(f"SERVER ERROR: Error processing {os.path.basename(file_path)}: {str(e)}")
        return {"status": "error", "message": str(e)}

def process_files_chronologically(file_paths, ticker, range_type_for_partial_updates, client_id, start_dt, end_dt, include_indicators):
    print(f"SERVER LOG: Processing {len(file_paths)} files for {ticker}")
    
    sorted_files = sorted(file_paths)
    max_workers = min(os.cpu_count() or 1, len(sorted_files))
    
    results = {}
    successful_files = []
    failed_files = []
    processed_count = 0
    total_count = len(sorted_files)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file_path in sorted_files:
            future = executor.submit(process_single_file, file_path, ticker, include_indicators)
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
                send_sse_message(client_id, 'error', {'file': file_date, 'message': str(e)})
    
    send_sse_message(client_id, 'processing_summary', {
        'total_files': total_count,
        'successful_files': successful_files,
        'failed_files': failed_files
    })
            
    print(f"SERVER LOG: Processing complete. Success: {len(successful_files)}, Failed: {len(failed_files)}")
    return results

def combine_results_in_order(results):
    if not results:
        return {"candlestick": [], "volume": []}

    # Dynamically find all data keys (candlestick, volume, and any indicators)
    all_keys = set()
    for date in results:
        if results[date].get("status") == "success":
            all_keys.update(results[date].keys())
    
    combined = {key: [] for key in all_keys if key != "status"}

    for date in sorted(results.keys()):
        if results[date]["status"] == "success":
            for key in combined:
                if key in results[date]:
                    combined[key].extend(results[date][key])
    
    # Sort all data series by time
    for key in combined:
        if combined[key] and isinstance(combined[key][0], dict) and 'time' in combined[key][0]:
            combined[key].sort(key=lambda x: x['time'])
    
    print(f"SERVER LOG: Combined {len(combined.get('candlestick', []))} data points.")
    return combined

def aggregate_data_by_type(data, range_type, data_type):
    if not data:
        return []
        
    aggregated_data = {}
    
    for data_point in data:
        if 'time' not in data_point:
            continue
            
        date = datetime.fromtimestamp(data_point['time'], tz=pytz.timezone('America/New_York'))
        
        if range_type == 'D':
            start_of_period = date.replace(hour=0, minute=0, second=0, microsecond=0)
            key = f"{start_of_period.year}-{start_of_period.month:02d}-{start_of_period.day:02d}"
        elif range_type == '1W':
            weekday = date.weekday()
            start_of_period = date - timedelta(days=weekday)
            key = f"{start_of_period.year}-W{start_of_period.isocalendar()[1]}"
            start_of_period = start_of_period.replace(hour=0, minute=0, second=0, microsecond=0)
        elif range_type == '1M':
            start_of_period = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            key = f"{start_of_period.year}-M{start_of_period.month:02d}"
        elif range_type == '1Y':
            start_of_period = date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            key = f"{start_of_period.year}"
        else:
            key = str(data_point['time'])
            start_of_period = date
            
        time_key = int(start_of_period.timestamp())
        
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
                })
            elif data_type == 'volume':
                aggregated_data[key].update({
                    'value': 0,
                    'up_volume': 0,
                    'down_volume': 0,
                })
            elif data_type == 'indicator':
                aggregated_data[key].update({
                    'values': [],
                })
        
        aggregated_data[key]['data_points'].append(data_point)
        
        if data_type == 'candlestick':
            if data_point['time'] < aggregated_data[key]['first_time']:
                aggregated_data[key]['open'] = data_point.get('open', aggregated_data[key]['open'])
                aggregated_data[key]['first_time'] = data_point['time']
                
            aggregated_data[key]['close'] = data_point.get('close', aggregated_data[key]['close'])
            
            if data_point.get('high', 0) > aggregated_data[key]['high']:
                aggregated_data[key]['high'] = data_point.get('high', 0)
            if data_point.get('low', float('inf')) < aggregated_data[key]['low']:
                aggregated_data[key]['low'] = data_point.get('low', aggregated_data[key]['low'])
                
        elif data_type == 'volume':
            value = data_point.get('value', 0)
            aggregated_data[key]['value'] += value
            
            if data_point.get('color') == '#26a69a':
                aggregated_data[key]['up_volume'] += value
            else:
                aggregated_data[key]['down_volume'] += value
        elif data_type == 'indicator':
            # Industry standard: Use last value for EMA/VWAP in aggregated timeframes
            if 'value' in data_point and data_point['value'] is not None:
                aggregated_data[key]['values'].append(data_point['value'])
    
    result = []
    for key, aggregate in aggregated_data.items():
        if data_type == 'candlestick':
            result.append({
                'time': aggregate['time'],
                'open': aggregate['open'],
                'high': aggregate['high'],
                'low': aggregate['low'] if aggregate['low'] != float('inf') else aggregate['open'],
                'close': aggregate['close'],
            })
        elif data_type == 'volume':
            color = '#26a69a' if aggregate['up_volume'] >= aggregate['down_volume'] else '#ef5350'
            result.append({
                'time': aggregate['time'],
                'value': aggregate['value'],
                'color': color
            })
        elif data_type == 'indicator':
            # Use the last non-null value
            value = aggregate['values'][-1] if aggregate['values'] else None
            result.append({'time': aggregate['time'], 'value': value})
    
    result.sort(key=lambda x: x['time'])
    return result

def aggregate_data(combined_data, range_type, start_dt, end_dt):
    candlestick_data = aggregate_data_by_type(combined_data.get("candlestick", []), range_type, 'candlestick')
    volume_data = aggregate_data_by_type(combined_data.get("volume", []), range_type, 'volume')
    
    result = {
        "candlestick": candlestick_data,
        "volume": volume_data
    }
    
    # Dynamically find and aggregate all other (indicator) data series
    standard_keys = {"candlestick", "volume", "processed_dates", "status"}
    indicator_keys = [k for k in combined_data.keys() if k not in standard_keys]

    for ind in indicator_keys:
        indicator_data = aggregate_data_by_type(combined_data[ind], range_type, 'indicator')
        result[ind] = indicator_data
    
    print(f"SERVER LOG: Aggregated to {range_type}, {len(candlestick_data)} candles")
    return result

@app.route('/sse-connect')
def sse_connect():
    def event_stream():
        client_queue = queue.Queue()
        client_id = str(time.time())
        
        with sse_clients_lock:
            sse_clients[client_id] = client_queue
        
        print(f"SERVER LOG: SSE Client Connected: {client_id}")
        yield f"data: {json.dumps({'client_id': client_id, 'type': 'connected'})}\n\n"
        
        try:
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield f"data: {json.dumps(message)}\n\n"
                    
                    if message.get('type') == 'complete':
                        break
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with sse_clients_lock:
                if client_id in sse_clients:
                    del sse_clients[client_id]
            print(f"SERVER LOG: SSE Client Disconnected: {client_id}")
    
    return Response(stream_with_context(event_stream()), mimetype='text/event-stream')

def send_sse_message(client_id, message_type, data):
    if client_id and client_id != 'background_loader':
        with sse_clients_lock:
            if client_id in sse_clients:
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

def send_partial_data_update(client_id, results, range_type, start_dt, end_dt):
    if range_type == '1D':
        combined_data = combine_results_in_order(results)
        send_sse_message(client_id, 'partial_data', combined_data)

def process_files_and_send_updates(files, ticker, requested_range_type, client_id, start_dt, end_dt, include_indicators):
    print(f"SERVER LOG: Background processing for {ticker}, range {requested_range_type}")
    try:
        results = process_files_chronologically(sorted(files), ticker, requested_range_type, client_id, start_dt, end_dt, include_indicators)
        
        if not results:
            send_sse_message(client_id, 'error', {'message': "No data processed for the selected date range"})
            return
        
        combined_data = combine_results_in_order(results)
        
        if not combined_data.get("candlestick"):
            send_sse_message(client_id, 'error', {'message': "No valid price data found"})
            return
        
        result_dates = sorted(list(results.keys()))
        
        if requested_range_type != '1D':
            final_data = aggregate_data(combined_data, requested_range_type, start_dt, end_dt)
        else:
            final_data = combined_data
        
        final_data["processed_dates"] = result_dates
        send_sse_message(client_id, 'complete', final_data)
        print(f"SERVER LOG: Final data sent to {client_id}")
            
    except Exception as e:
        print(f"SERVER ERROR: Background processing error: {str(e)}")
        send_sse_message(client_id, 'error', {'message': f"Server error: {str(e)}"})

@app.route("/")
def index():
    return redirect(url_for('chart_page'))

@app.route("/chart")
def chart_page():
    response = make_response(send_from_directory(".", "index.html"))
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response

@app.route("/favicon.ico")
def favicon():
    return '', 204

@app.route("/one_minute_data_range", methods=["GET"])
def one_minute_data_range():
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        ticker = request.args.get("ticker", "MSFT")
        requested_range_type = request.args.get("range", "1D")
        client_id = request.args.get("client_id")
        # The 'indicators' flag is no longer needed but we accept it to avoid breaking the client
        include_indicators = request.args.get("indicators", "true").lower() == "true"

        if not start_date:
            return jsonify({"error": "start_date is required"}), 400
            
        if not end_date:
            end_date = start_date

        # Cache key still includes indicator flag to differentiate requests if client sends it
        cache_key = f"{ticker}-{start_date}-{end_date}-{requested_range_type}-ind{include_indicators}"
        
        if cache_key in SERVER_CACHE:
            print(f"SERVER LOG: Cache hit for {cache_key}")
            return jsonify(SERVER_CACHE[cache_key])

        cache_key_1D = f"{ticker}-{start_date}-{end_date}-1D-ind{include_indicators}"
        if requested_range_type != '1D' and cache_key_1D in SERVER_CACHE:
            print(f"SERVER LOG: Aggregating from cached 1D data")
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            aggregated_data = aggregate_data(SERVER_CACHE[cache_key_1D], requested_range_type, start_dt, end_dt)
            SERVER_CACHE[cache_key] = aggregated_data
            return jsonify(aggregated_data)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        files_to_process = []
        for file_path in find_csv_gz_files(ONE_MIN_DIR):
            try:
                file_date = datetime.strptime(os.path.basename(file_path).replace(".csv.gz", ""), "%Y-%m-%d")
                if start_dt <= file_date <= end_dt:
                    files_to_process.append(file_path)
            except Exception:
                continue
        
        if not files_to_process:
            return jsonify({"error": "No data files found"}), 404

        with sse_clients_lock:
            client_exists = client_id in sse_clients

        if client_exists:
            thread = threading.Thread(
                target=process_files_and_send_updates,
                args=(files_to_process, ticker, requested_range_type, client_id, start_dt, end_dt, include_indicators)
            )
            thread.daemon = True
            thread.start()
            return jsonify({"status": "processing_started", "files_count": len(files_to_process)})
        else:
            # Synchronous processing for clients not using SSE
            results = process_files_chronologically(files_to_process, ticker, '1D', 'sync', start_dt, end_dt, include_indicators)
            combined_data = combine_results_in_order(results)

            if requested_range_type != '1D':
                final_data = aggregate_data(combined_data, requested_range_type, start_dt, end_dt)
            else:
                final_data = combined_data
            
            SERVER_CACHE[cache_key] = final_data
            return jsonify(final_data)
            
    except Exception as e:
        print(f"SERVER ERROR: {str(e)}")
        return jsonify({"error": f"Failed to load data: {str(e)}"}), 500

@app.route("/available_dates", methods=["GET"])
def available_dates():
    try:
        csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
        dates = [os.path.basename(f).replace(".csv.gz", "") for f in csv_gz_files]
        return jsonify({"dates": sorted(dates)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/available_tickers", methods=["GET"])
def available_tickers():
    date = request.args.get("date")
    if not date:
        return jsonify({"error": "Date required"}), 400
    
    csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
    file_path = None
    for file in csv_gz_files:
        if date in file:
            file_path = file
            break
    
    if not file_path:
        return jsonify({"error": f"No data for {date}"}), 404
    
    tickers = set()
    try:
        with gzip.open(file_path, "rt") as f:
            next(f)
            for line in f:
                fields = line.strip().split(',')
                if fields:
                    tickers.add(fields[0])
        return jsonify({"tickers": sorted(list(tickers))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = 8000
    print("=" * 60)
    print(f"Data directory: {DATA_DIR}")
    
    if not os.path.exists(DATA_DIR):
        print(f"WARNING: Data directory {DATA_DIR} does not exist!")
    else:
        csv_count = len(find_csv_gz_files(DATA_DIR))
        print(f"Found {csv_count} CSV.GZ files")
    
    print(f"Server starting on http://localhost:{port}/chart")
    print("=" * 60)
    
    try:
        from waitress import serve
        serve(app, host="0.0.0.0", port=port)
    except Exception as e:
        print(f"SERVER ERROR: {str(e)}")
        sys.exit(1)
