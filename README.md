## What is AugurData?

AugurData is a high-performance visualization platform for analyzing Polygon.io minute-level market data. It provides institutional-grade technical analysis capabilities through multi-timeframe aggregation, VWAP calculations, and chronological market replay to identify price action patterns and liquidity dynamics.

## Features

- **Multi-timeframe Analysis**: Raw minute data plus Daily, Weekly, Monthly, and Yearly aggregations
- **Data Source**: Accesses a survivorship-bias-free dataset from Polygon.io, providing historical and real-time data on a selection of stocks, including high-momentum stocks, and both actively traded and delisted securities.
- **Interactive Data**: Real-time OHLCV and VWAP values on hover
- **Historical Playback**: Time-slider for chronological market replay
- **Visual Customization**: Toggle between dark/light themes
- **Processing Transparency**: Real-time updates via Server-Sent Events
- **Robust Operation**: Automatic recovery from processing failures
- **Performance Optimized**: Efficient caching system minimizes redundant calculations
- **Direct Chart Linking**: Generate and access specific charts instantly via pre-configured URLs from command-line arguments.
- **Intelligent Data Pre-loading**: Automatically pre-load and cache minute-level data when starting the server with specific chart parameters, optimizing subsequent access.

### Installation and Setup

1.  **Install dependencies**: Open your terminal or command prompt and run the following command:
    ```bash
    pip install flask waitress pytz filelock
    ```
    *Note: `filelock` has been added as a dependency for robust file handling.*

2.  **Start the server**: Navigate to your project directory in your terminal or command prompt.

    You have two primary ways to start the server and access the charting interface:

    #### A. Direct URL Access (Recommended for Specific Charts)
    To instantly view a chart for a specific ticker and date range directly in your browser, start the server with command-line arguments. This will generate a direct URL for you.

    **Prompt Command:**
    ```bash
    python server.py [TICKER] [START_DATE_YYYY-MM-DD] [END_DATE_YYYY-MM-DD] [OPTIONAL_RANGE]
    ```
    -   `[TICKER]`: The stock ticker symbol (e.g., `MSFT`, `AAPL`).
    -   `[START_DATE_YYYY-MM-DD]`: The starting date for the data (e.g., `2016-01-04`).
    -   `[END_DATE_YYYY-MM-DD]`: The ending date for the data (e.g., `2016-01-05`). Can be the same as `START_DATE`.
    -   `[OPTIONAL_RANGE]`: The desired aggregation range. Options: `minutes`, `daily` (or `d`), `week` (or `w`, `1w`), `month` (or `m`, `1m`), `year` (or `y`, `1y`). Defaults to `minutes` if not specified.

    **Example:**
    ```bash
    python server.py MSFT 2016-01-04 2016-01-05 daily
    ```
    Upon running this command, the server will output a direct URL in your terminal. Open this URL in your browser:
    ```
    http://localhost:8000/chart?ticker=MSFT&from=2016-01-04&to=2016-01-05&range=D
    ```
    The chart will automatically load with the specified data once processing is complete.

    #### B. Standard Web Interface Access (for Flexible Exploration)
    To start the server and then select tickers and dates interactively through the web interface:

    **Prompt Command:**
    ```bash
    python server.py
    ```
    Once the server starts (you'll see a message like `Server running at http://localhost:8000/chart`), open your web browser and go to:

    **URL Access:**
    ```
    http://localhost:8000/chart
    ```
    From here, you can use the dropdowns and buttons to fetch and display data.
### Screenshots

minute raw data
![Screenshot 2025-03-28 215339](https://github.com/user-attachments/assets/68ee3e04-7eca-4cc2-8a02-0061dcde8431)


daily aggregate range
![Screenshot 2025-03-28 222428](https://github.com/user-attachments/assets/422de4c7-f1b1-413c-a108-b90fbf9bb08e)

