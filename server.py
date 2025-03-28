import os
import gzip
import json
import time
import queue
import threading
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
import pytz
import statistics
from pathlib import Path
from waitress import serve

app = Flask(__name__)

sse_clients = {}
DATA_DIR = None
ONE_MIN_DIR = None

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

def convert_epoch_to_ny_time(epoch_ns):
    try:
        epoch_seconds = epoch_ns / 1_000_000_000
        utc_time = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        ny_tz = pytz.timezone('America/New_York')
        ny_time = utc_time.astimezone(ny_tz)
        return int(ny_time.timestamp())
    except Exception as e:
        print(f"Error converting timestamp: {str(e)}")
        return None

def find_csv_gz_files(directory):
    csv_gz_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv.gz"):
                csv_gz_files.append(os.path.join(root, file))
    return csv_gz_files

def calculate_and_add_vwap(file_path, ticker):
    temp_file = file_path + '.tmp'
    lock_file = file_path + '.lock'
    
    try:
        if os.path.exists(lock_file):
            try:
                lock_time = os.path.getmtime(lock_file)
                current_time = time.time()
                if current_time - lock_time > 300:
                    print(f"Removing stale lock file for {file_path}")
                    os.remove(lock_file)
                else:
                    print(f"Another process is already working on {file_path}, skipping")
                    return False
            except Exception as e:
                print(f"Error checking lock file age: {str(e)}")
                return False
                
        try:
            with open(lock_file, 'w') as f:
                f.write(f"Locked by process {os.getpid()} at {time.time()}")
        except Exception as e:
            print(f"Cannot create lock file for {file_path}: {str(e)}")
            return False
        
        with gzip.open(file_path, "rt") as f:
            header = f.readline().strip()
            header_fields = header.split(',')
            
            vwap_exists = 'VWAP' in header_fields
            vwap_index = header_fields.index('VWAP') if vwap_exists else len(header_fields)
            
            ticker_has_vwap = False
            if vwap_exists:
                for line in f:
                    fields = line.strip().split(',')
                    if len(fields) <= 1:
                        continue
                    if fields[0] == ticker and vwap_index < len(fields) and fields[vwap_index].strip():
                        ticker_has_vwap = True
                        break
        
        if ticker_has_vwap:
            print(f"VWAP already exists for {ticker} in {file_path}")
            return True
        
        ticker_data = []
        with gzip.open(file_path, "rt") as f:
            next(f)
            
            for line in f:
                fields = line.strip().split(',')
                if len(fields) <= 6 or fields[0] != ticker:
                    continue
                
                try:
                    timestamp = int(fields[6])
                    open_price = float(fields[2])
                    close_price = float(fields[3])
                    high_price = float(fields[4])
                    low_price = float(fields[5])
                    volume = float(fields[1])
                    
                    ticker_data.append({
                        'timestamp': timestamp,
                        'open': open_price,
                        'close': close_price,
                        'high': high_price,
                        'low': low_price,
                        'volume': volume
                    })
                except (ValueError, IndexError) as e:
                    print(f"Skipping invalid data point: {line.strip()} - {str(e)}")
                    continue
        
        if not ticker_data:
            print(f"No valid data found for {ticker} in {file_path}")
            return False
        
        ticker_data.sort(key=lambda x: x['timestamp'])
        
        cumulative_tp_volume = 0
        cumulative_volume = 0
        vwap_values = {}
        
        for point in ticker_data:
            typical_price = (point['high'] + point['low'] + point['close']) / 3
            cumulative_tp_volume += typical_price * point['volume']
            cumulative_volume += point['volume']
            
            if cumulative_volume > 0:
                vwap = cumulative_tp_volume / cumulative_volume
            else:
                vwap = typical_price
            
            vwap_values[point['timestamp']] = vwap
        
        with gzip.open(file_path, "rt") as src, gzip.open(temp_file, "wt") as dst:
            header_line = src.readline().strip()
            if not vwap_exists:
                header_line += ',VWAP'
                vwap_index = len(header_fields)
            dst.write(header_line + '\n')
            
            for line in src:
                original_line = line
                fields = line.strip().split(',')
                
                if len(fields) <= 6 or fields[0] != ticker:
                    dst.write(original_line)
                    continue
                
                try:
                    timestamp = int(fields[6])
                    if timestamp in vwap_values:
                        while len(fields) <= vwap_index:
                            fields.append('')
                        
                        fields[vwap_index] = str(vwap_values[timestamp])
                        dst.write(','.join(fields) + '\n')
                    else:
                        dst.write(original_line)
                except (ValueError, IndexError):
                    dst.write(original_line)
        
        try:
            os.replace(temp_file, file_path)
            print(f"VWAP CALCULATION COMPLETED: {ticker} in {file_path}")
            return True
        except Exception as e:
            print(f"ERROR REPLACING FILE: {str(e)}")
            return False
        
    except Exception as e:
        print(f"ERROR CALCULATING VWAP for {ticker} in {file_path}: {str(e)}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass
        return False
    finally:
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception as e:
            print(f"WARNING: Could not remove lock file {lock_file}: {str(e)}")


def process_single_file(file_path, ticker):
    try:
        vwap_added = calculate_and_add_vwap(file_path, ticker)
        
        if not vwap_added:
            return {"status": "error", "message": f"Failed to calculate VWAP for {ticker}"}
            
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

                        if has_vwap and vwap_index < len(fields) and fields[vwap_index]:
                            try:
                                vwap_value = float(fields[vwap_index])
                                vwap_data.append({
                                    "time": time_in_seconds,
                                    "value": vwap_value,
                                    "exactTime": True
                                })
                            except (ValueError, IndexError):
                                pass
                    except (ValueError, IndexError) as e:
                        print(f"Skipping invalid line: {line.strip()} | Error: {str(e)}")
        
        candlestick_data.sort(key=lambda x: x["time"])
        volume_data.sort(key=lambda x: x["time"])
        vwap_data.sort(key=lambda x: x["time"])
        
        file_date = os.path.basename(file_path).replace('.csv.gz', '')
        print(f"FILE PROCESSED SUCCESSFULLY: {file_date} ({len(candlestick_data)} data points)")
        
        return {
            "status": "success",
            "candlestick": candlestick_data,
            "volume": volume_data,
            "vwap": vwap_data
        }
        
    except Exception as e:
        print(f"ERROR PROCESSING FILE {file_path}: {str(e)}")
        return {"status": "error", "message": str(e)}

def process_files_chronologically(file_paths, ticker, range_type, client_id, start_dt, end_dt):
    sorted_files = sorted(file_paths)
    max_workers = min(os.cpu_count(), len(sorted_files))
    
    print(f"PROCESSING {len(sorted_files)} FILES")
    
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
                result = future.result()
                processed_count += 1
                
                if result["status"] == "success":
                    results[file_date] = result
                    successful_files.append(file_date)
                    print(f"COMPLETED {processed_count}/{total_count}: {file_date}")
                else:
                    failed_files.append(file_date)
                    print(f"FAILED TO PROCESS: {file_date} - {result.get('message', 'Unknown error')}")
                
                send_progress_update(client_id, file_date, processed_count, total_count)
                
                if result["status"] == "success":
                    send_partial_data_update(client_id, results, range_type, start_dt, end_dt)
                
            except Exception as e:
                processed_count += 1
                failed_files.append(file_date)
                print(f"ERROR PROCESSING {file_date}: {str(e)}")
                send_sse_message(client_id, 'error', {
                    'file': file_date,
                    'message': str(e)
                })
    
    send_sse_message(client_id, 'processing_summary', {
        'total_files': total_count,
        'successful_files': successful_files,
        'failed_files': failed_files
    })
    
    print(f"COMPLETED PROCESSING ALL {total_count} FILES")
    print(f"SUCCESSFULLY PROCESSED: {len(successful_files)} FILES")
    print(f"FAILED TO PROCESS: {len(failed_files)} FILES")
    if failed_files:
        print(f"FAILED FILES: {', '.join(failed_files)}")
            
    return results

def process_date_files_in_parallel(file_paths, ticker, client_id):
    print("WARNING: Using deprecated process_date_files_in_parallel function")
    print("Please use process_files_chronologically instead")
    
    results = {}
    processed = 0
    total = len(file_paths)
    max_workers = min(os.cpu_count(), len(file_paths))
    
    successful_files = []
    failed_files = []
    
    print(f"PROCESSING {len(file_paths)} FILES WITH {max_workers} WORKERS")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file_path in file_paths:
            future = executor.submit(process_single_file, file_path, ticker)
            futures.append((future, file_path))
            
            time.sleep(0.1)
        
        for future, file_path in futures:
            file_date = os.path.basename(file_path).replace('.csv.gz', '')
            processed += 1
            
            try:
                print(f"COMPLETED {processed}/{total}: {file_date}")
                result = future.result()
                
                if result["status"] == "success":
                    results[file_date] = result
                    successful_files.append(file_date)
                else:
                    failed_files.append(file_date)
                    print(f"FAILED TO PROCESS: {file_date} - {result.get('message', 'Unknown error')}")
                
                send_progress_update(client_id, file_date, processed, total)
                
                if result["status"] == "success":
                    send_partial_data_update(client_id, results)
                
            except Exception as e:
                print(f"ERROR PROCESSING {file_path}: {str(e)}")
                failed_files.append(file_date)
                send_sse_message(client_id, 'error', {
                    'file': file_date,
                    'message': str(e)
                })
    
    send_sse_message(client_id, 'processing_summary', {
        'total_files': total,
        'successful_files': successful_files,
        'failed_files': failed_files
    })
    
    print(f"COMPLETED PROCESSING ALL {total} FILES IN PARALLEL")
    print(f"SUCCESSFULLY PROCESSED: {len(successful_files)} FILES")
    print(f"FAILED TO PROCESS: {len(failed_files)} FILES")
    if failed_files:
        print(f"FAILED FILES: {', '.join(failed_files)}")
            
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
    
    print(f"DATA COMBINED: {len(all_candlesticks)} candlesticks from {len(dates)} dates")
    
    return {
        "candlestick": all_candlesticks,
        "volume": all_volumes,
        "vwap": all_vwaps
    }

def aggregate_data_by_type(data, range_type, data_type, start_date=None, end_date=None):
    if not data:
        return []
    
    reference_year = None
    if start_date:
        if isinstance(start_date, str):
            reference_year = datetime.strptime(start_date, "%Y-%m-%d").year
        elif isinstance(start_date, datetime):
            reference_year = start_date.year
        
    aggregated_data = {}
    
    for data_point in data:
        if 'time' not in data_point:
            continue
            
        date = datetime.fromtimestamp(data_point['time'], tz=pytz.timezone('America/New_York'))
        
        if reference_year and range_type == '1Y':
            date = date.replace(year=reference_year)
        
        if range_type == 'D':
            # Group by calendar day
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
            key = f"{start_of_month.year}-M{start_of_month.month}"
            time_key = int(start_of_month.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        elif range_type == '1Y':
            start_of_year = date.replace(month=1, day=1)
            if reference_year:
                start_of_year = start_of_year.replace(year=reference_year)
            key = f"{start_of_year.year}"
            time_key = int(start_of_year.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
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
                if aggregated_data[key]['low'] == float('inf'):
                    aggregated_data[key]['low'] = data_point.get('low', 0)
            elif data_type == 'volume':
                aggregated_data[key].update({
                    'value': 0,
                    'up_volume': 0,
                    'down_volume': 0,
                })
            elif data_type == 'vwap':
                aggregated_data[key].update({
                    'value': 0,
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
            if data_point.get('low', float('inf')) < aggregated_data[key]['low'] and data_point.get('low', 0) > 0:
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
    candlestick_set = []
    volume_set = []
    vwap_set = []

    for candle in combined_data["candlestick"]:
        temp = {key: value for key, value in candle.items()}
        candlestick_set.append(temp)
        
    for vol in combined_data["volume"]:
        temp = {key: value for key, value in vol.items()}
        volume_set.append(temp)
        
    for vwap in combined_data["vwap"]:
        temp = {key: value for key, value in vwap.items()}
        vwap_set.append(temp)
    
    print(f"AGGREGATING DATA FOR {range_type}: {len(candlestick_set)} points to aggregate")
        
    candlestick_data = aggregate_data_by_type(candlestick_set, range_type, 'candlestick', start_dt, end_dt)
    volume_data = aggregate_data_by_type(volume_set, range_type, 'volume', start_dt, end_dt)
    vwap_data = aggregate_data_by_type(vwap_set, range_type, 'vwap', start_dt, end_dt)
    
    print(f"AGGREGATION COMPLETE: {len(candlestick_data)} {range_type} bars created")
    
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
            if client_id in sse_clients:
                del sse_clients[client_id]
    
    return Response(stream_with_context(event_stream()), 
                   mimetype='text/event-stream')

def send_sse_message(client_id, message_type, data):
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

def send_partial_data_update(client_id, results, range_type=None, start_dt=None, end_dt=None):
    combined_data = combine_results_in_order(results)
    
    if range_type and range_type != '1D' and start_dt and end_dt:
        partial_data = aggregate_data(combined_data, range_type, start_dt, end_dt)
        print(f"PARTIAL AGGREGATED DATA UPDATE SENT: {len(partial_data['candlestick'])} {range_type} bars")
        send_sse_message(client_id, 'partial_data', partial_data)
    else:
        print(f"PARTIAL DATA UPDATE SENT: {len(combined_data['candlestick'])} data points")
        send_sse_message(client_id, 'partial_data', combined_data)

def process_files_and_send_updates(files, ticker, range_type, client_id, start_dt, end_dt):
    try:
        sorted_files = sorted(files)
        total_files = len(sorted_files)
        
        results = process_files_chronologically(sorted_files, ticker, range_type, client_id, start_dt, end_dt)
        
        if not results:
            print("NO VALID DATA WAS PROCESSED SUCCESSFULLY")
            send_sse_message(client_id, 'error', {
                'message': "No data could be processed successfully for the selected date range"
            })
            return
        
        print("COMBINING RESULTS")
        combined_data = combine_results_in_order(results)
        
        if not combined_data["candlestick"]:
            print("NO CANDLESTICK DATA AVAILABLE AFTER COMBINING RESULTS")
            send_sse_message(client_id, 'error', {
                'message': "No valid price data found for the selected ticker and date range"
            })
            return
        
        result_dates = sorted(list(results.keys()))
        
        if range_type != '1D':
            print(f"AGGREGATING DATA FOR {range_type} VIEW")
            aggregated_data = aggregate_data(combined_data, range_type, start_dt, end_dt)
            aggregated_data["processed_dates"] = result_dates
            print("SENDING FINAL AGGREGATED DATA TO CLIENT")
            send_sse_message(client_id, 'complete', aggregated_data)
        else:
            combined_data["processed_dates"] = result_dates
            print("SENDING FINAL COMBINED DATA TO CLIENT")
            send_sse_message(client_id, 'complete', combined_data)
        
        print("BACKGROUND PROCESSING COMPLETED SUCCESSFULLY")
            
    except Exception as e:
        print(f"ERROR IN BACKGROUND PROCESSING: {str(e)}")
        send_sse_message(client_id, 'error', {
            'message': str(e)
        })

@app.route("/")
def index():
    return send_from_directory(".", "index.html")

@app.route("/one_minute_data_range", methods=["GET"])
def one_minute_data_range():
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        ticker = request.args.get("ticker", "MSFT")
        range_type = request.args.get("range", "1D")
        client_id = request.args.get("client_id")

        if not start_date:
            return jsonify({"error": "start_date is required"}), 400
            
        if not end_date:
            end_date = start_date
        
        print(f"PROCESSING REQUEST: ticker={ticker}, range={range_type}, from {start_date} to {end_date}")
            
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
                print(f"Error parsing file date: {file_path} - {str(e)}")
        
        files_to_process.sort()
        
        print(f"FILES TO PROCESS: {len(files_to_process)}")
        
        if len(files_to_process) == 1:
            result = process_single_file(files_to_process[0], ticker)
            
            if result["status"] == "error":
                return jsonify({"error": result["message"]}), 500
                
            return jsonify(result)
        
        if client_id in sse_clients:
            thread = threading.Thread(
                target=process_files_and_send_updates,
                args=(files_to_process, ticker, range_type, client_id, start_dt, end_dt)
            )
            thread.daemon = True
            thread.start()
            
            return jsonify({
                "status": "processing_started",
                "message": "Processing started, connect to SSE for updates",
                "files_count": len(files_to_process)
            })
        else:
            return jsonify({
                "error": "No SSE connection established. Connect to /sse-connect first."
            }), 400
            
    except Exception as e:
        print(f"SERVER ERROR: {str(e)}")
        return jsonify({"error": f"Failed to load data: {str(e)}"}), 500

@app.route("/available_dates", methods=["GET"])
def available_dates():
    dates = []
    try:
        csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
        for file_path in csv_gz_files:
            file_name = os.path.basename(file_path)
            date_str = file_name.replace(".csv.gz", "")
            dates.append(date_str)
        return jsonify({"dates": sorted(dates)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/available_tickers", methods=["GET"])
def available_tickers():
    date = request.args.get("date", "2016-01-04")
    csv_gz_files = find_csv_gz_files(ONE_MIN_DIR)
    file_path = None
    for file in csv_gz_files:
        if date in file:
            file_path = file
            break
    if not file_path:
        return jsonify({"error": f"No data found for {date}"}), 404
    tickers = set()
    with gzip.open(file_path, "rt") as f:
        next(f)
        for line in f:
            fields = line.strip().split(',')
            if fields:
                tickers.add(fields[0])
    return jsonify({"tickers": sorted(list(tickers))})

def print_startup_message():
    print("=" * 60)
    print("=" * 60)
    print(f"Working directory: {os.getcwd()}")
    print(f"Data directory: {DATA_DIR}")
    
    if not os.path.exists(DATA_DIR):
        print(f"WARNING: Data directory {DATA_DIR} does not exist!")
    else:
        csv_gz_count = len(find_csv_gz_files(DATA_DIR))
        print(f"Found {csv_gz_count} CSV.GZ files in the data directory")
        
    print("=" * 60)

if __name__ == "__main__":
    print_startup_message()
    
    port = 8000
    print(f"Running server on port {port}")
    print(f"Server running at http://localhost:{port}")
    serve(app, host="0.0.0.0", port=port)