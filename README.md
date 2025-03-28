## What is AugurData?

AugurData is a high-performance visualization platform for analyzing Polygon.io minute-level market data. It provides institutional-grade technical analysis capabilities through multi-timeframe aggregation, VWAP calculations, and chronological market replay to identify price action patterns and liquidity dynamics.

## Features

- **Multi-timeframe Analysis**: Raw minute data plus Daily, Weekly, Monthly, and Yearly aggregations
- **Comprehensive Coverage**: Support for both listed and delisted securities
- **Interactive Data**: Real-time OHLCV and VWAP values on hover
- **Historical Playback**: Time-slider for chronological market replay
- **Visual Customization**: Toggle between dark/light themes
- **Processing Transparency**: Real-time updates via Server-Sent Events
- **Robust Operation**: Automatic recovery from processing failures
- **Performance Optimized**: Efficient caching system minimizes redundant calculations

## Getting Started

### Prerequisites
- Python 3.10 or higher
- Polygon.io minute-level data in CSV.GZ format

Install dependencies
```
pip install flask waitress pytz
```
Start server
```
python server.py
```
Open in your browser
```
http://localhost:8000 
```
### Screenshots

minute raw data
![Screenshot 2025-03-28 215339](https://github.com/user-attachments/assets/68ee3e04-7eca-4cc2-8a02-0061dcde8431)


daily aggregate range
![Screenshot 2025-03-28 222428](https://github.com/user-attachments/assets/422de4c7-f1b1-413c-a108-b90fbf9bb08e)

