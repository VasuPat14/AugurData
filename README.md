## What is AugurData?

AugurData is a high-performance visualization platform designed for analyzing high-resolution, local market data, such as the minute-level datasets provided by vendor **massive.com**. It provides an interactive, multi-window interface for technical analysis with dynamic indicator loading and multi-timeframe aggregation.

## Features

-   **Multi-Window Interface**: Open, drag, and resize multiple chart windows, one for each ticker, for simultaneous analysis.
-   **Dual Analysis Modes**: Instantly switch between the multi-window "Charts" mode and a single-chart "Series Compare" mode.
-   **Dynamic Indicator Loading**: Automatically detects and visualizes all indicators present in the data files (e.g., VWAP, EMAs).
-   **Contextual Settings Panel**: Configure each chart's properties after data is loaded. Toggle between Chart Types (Candlestick/Line) and manage the visibility of all loaded indicators.
-   **Multi-Timeframe Aggregation**: Analyze raw minute data or view aggregated Daily, Weekly, Monthly, and Yearly timeframes.
-   **Interactive Hover Legend**: See real-time OHLCV and all active indicator values on crosshair hover.
-   **Historical Playback**: Use the time-slider at the bottom of each chart for chronological market replay.
-   **Data Selection**: Quickly find and select data using searchable dropdowns for available dates and tickers.
-   **Performance Optimized**: Features parallel file processing on the backend and an in-memory server cache to speed up subsequent data loads.
-   **Live Processing Updates**: A real-time progress indicator shows data-loading status via Server-Sent Events (SSE).
-   **Visual Customization**: Toggle between dark and light themes.

### Installation and Setup

1.  **Install dependencies**: Open your terminal or command prompt and run the following command:
    ```bash
    pip install flask waitress pytz filelock
    ```
    *(Note: `waitress` is used to serve the application, and `filelock` ensures robust file handling.)*

2.  **Set Data Directory**: The server needs to know where your `.csv.gz` files are.
    * **Option A (Recommended)**: Set the `STOCK_DATA_DIR` environment variable to your data directory path (e.g., `L:\minutes`).
    * **Option B**: If the environment variable is not set, the server will prompt you to enter the path in your terminal the first time you run it.

3.  **Start the server**: Navigate to your project directory in your terminal and run:
    ```bash
    python server.py
    ```

4.  **Access the Interface**: Once the server is running, open your web browser and go to:
    ```
    http://localhost:8000/chart
    ```
    From here, you can use the dropdowns and buttons to fetch and display data.

### Screenshots

*(Example of charts-minute raw data)*
<img width="1855" height="1273" alt="Screenshot 2025-10-31 232040" src="https://github.com/user-attachments/assets/85f676a0-7a0f-4453-a7ad-af4bba0872cf" />

*(Example of series compare-minute raw data)*
<img width="2535" height="1272" alt="Screenshot 2025-11-01 004803" src="https://github.com/user-attachments/assets/b9848358-42f6-4adb-847f-d94aa8cf4367" />

---


