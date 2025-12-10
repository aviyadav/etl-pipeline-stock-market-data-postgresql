# ETL Pipeline for Stock Market Data (PostgreSQL)

This project provides an ETL (Extract, Transform, Load) pipeline to fetch stock market data from the Alpha Vantage API and store it in a PostgreSQL database. It supports parallel processing for efficient data ingestion.

## Features
- Fetches daily, intraday, and SMA indicator data for multiple stock symbols
- Stores data in normalized PostgreSQL tables
- Uses connection pooling for efficient DB access
- Parallel processing with Python's `concurrent.futures` for faster ETL
- Robust error handling and logging

## Requirements
- Python 3.8+
- PostgreSQL database (e.g., Neon, local, or cloud)
- Alpha Vantage API key
- Python packages: `psycopg2`, `requests`, `python-dotenv`

## Setup
1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure environment variables**: Create a `.env` file in the project root with the following:
   ```ini
   DB_HOST=your_postgres_host
   DB_NAME=your_db_name
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_PORT=5432
   ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
   ```
   For Neon/PostgreSQL cloud, set `DB_HOST` to your Neon endpoint and ensure SSL is enabled.

## Usage

### Parallel ETL Pipeline
Run the parallel ETL pipeline with:
```bash
python main_parallel.py
```
- The script will create required tables, ensure company records exist, and fetch/process data for all configured symbols and endpoints in parallel.
- Results and errors are logged to `etl_log.log` and the console.

### Customization
- Edit the `symbols` and `endpoints` lists in `main_parallel.py` to change which stocks or data types are processed.
- Adjust `max_workers` in `main_parallel.py` to control parallelism (be mindful of Alpha Vantage API rate limits).

## Table Structure
- `companies`: List of stock symbols
- `daily_stock_prices`: Daily OHLCV data
- `intraday_stock_prices`: 5-minute interval OHLCV data
- `sma_indicators`: Simple Moving Average indicators

## Notes
- The free Alpha Vantage API tier allows 5 requests per minute. The script is configured to respect this limit.
- For large-scale or production use, consider upgrading your API plan and database resources.

## License
MIT
