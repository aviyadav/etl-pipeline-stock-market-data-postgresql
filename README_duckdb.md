# ETL Pipeline for Stock Market Data (DuckDB)

This project provides an ETL (Extract, Transform, Load) pipeline to fetch stock market data from the Alpha Vantage API and store it in a DuckDB database. It supports parallel processing for efficient data ingestion.

## Features
- Fetches daily, intraday, and SMA indicator data for multiple stock symbols
- Stores data in normalized DuckDB tables (embedded analytical database)
- Thread-local connections for efficient multi-threaded access
- Parallel processing with Python's `concurrent.futures` for faster ETL
- Robust error handling and logging
- Lightweight and portable - database is a single file

## Requirements
- Python 3.8+
- DuckDB (embedded database - no server needed)
- Alpha Vantage API key
- Python packages: `duckdb`, `requests`, `python-dotenv`

## Setup
1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install duckdb requests python-dotenv
   ```
   Or update `pyproject.toml` and install:
   ```bash
   pip install -e .
   ```
3. **Configure environment variables**: Create a `.env` file in the project root with the following:
   ```ini
   ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
   DUCKDB_PATH=stock_market.duckdb  # Optional, defaults to stock_market.duckdb
   ```

## Usage

### Parallel ETL Pipeline (Recommended)
Run the parallel ETL pipeline with:
```bash
python main_parallel_duckdb.py
```
- The script will create required tables, ensure company records exist, and fetch/process data for all configured symbols and endpoints in parallel.
- Results and errors are logged to `etl_log_duckdb.log` and the console.
- Data is stored in `stock_market.duckdb` file

### Simple Sequential ETL Pipeline
Run the sequential ETL pipeline with:
```bash
python main_duckdb.py
```
- Processes symbols one at a time (slower but simpler)

### Customization
- Edit the `symbols` and `endpoints` lists in `main_parallel_duckdb.py` to change which stocks or data types are processed.
- Adjust `max_workers` in `main_parallel_duckdb.py` to control parallelism (be mindful of Alpha Vantage API rate limits).

## Table Structure
- `companies`: List of stock symbols
- `daily_stock_prices`: Daily OHLCV (Open, High, Low, Close, Volume) data
- `intraday_stock_prices`: Intraday OHLCV data (5-minute intervals)
- `sma_indicators`: Simple Moving Average indicators

## Key Differences from PostgreSQL Version
1. **No server setup required**: DuckDB is an embedded database - just a single file
2. **Thread-local connections**: Each thread maintains its own connection
3. **Different SQL syntax**:
   - `INSERT OR IGNORE` instead of `INSERT ... ON CONFLICT DO NOTHING`
   - Parameterized queries use `?` instead of `%s`
4. **File-based storage**: All data stored in a single `.duckdb` file
5. **Excellent for analytics**: DuckDB is optimized for analytical queries (aggregations, window functions, etc.)

## Querying the Database
You can query the DuckDB database using Python:
```python
import duckdb

conn = duckdb.connect('stock_market.duckdb')
result = conn.execute("""
    SELECT company_symbol, date, close_price 
    FROM daily_stock_prices 
    WHERE company_symbol = 'AAPL' 
    ORDER BY date DESC 
    LIMIT 10
""").fetchall()
print(result)
conn.close()
```

Or use the DuckDB CLI:
```bash
duckdb stock_market.duckdb
```

## Performance Notes
- DuckDB is optimized for analytical workloads and can be faster than PostgreSQL for many read operations
- The parallel version uses thread-local connections to avoid connection contention
- Alpha Vantage free tier limits: 5 calls/minute, 500 calls/day
- Adjust `max_workers` to stay within API limits (recommended: 3)

## Logging
- All ETL operations are logged to `etl_log_duckdb.log`
- Console output shows real-time progress
- Each thread's operations are tracked separately

## License
MIT License
