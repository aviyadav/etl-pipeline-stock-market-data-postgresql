import logging
import datetime
import requests
import json
import duckdb
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Global connection
conn = None
cursor = None

def init_connection():
    """Initialize DuckDB connection."""
    global conn, cursor
    db_path = os.environ.get("DUCKDB_PATH", "stock_market.duckdb")
    conn = duckdb.connect(db_path)
    cursor = conn.cursor()
    logging.info(f"Connected to DuckDB database: {db_path}")

def create_tables():
    tables = []
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS companies (
        company_symbol VARCHAR(10) PRIMARY KEY
        );
    """)
    
    tables.append('companies')
    print("companies table created!")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_stock_prices (
        company_symbol varchar(10),
        date DATE,
        open_price DECIMAL(15, 4) NOT NULL,
        high_price DECIMAL(15, 4) NOT NULL,
        low_price DECIMAL(15, 4) NOT NULL,
        close_price DECIMAL(15, 4) NOT NULL,
        volume BIGINT NOT NULL,
        PRIMARY KEY (company_symbol, date),
        FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        );
    """)
    
    tables.append('daily_stock_prices')
    print("daily_stock_prices table created!")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS intraday_stock_prices (
        company_symbol varchar(10),
        date_time TIMESTAMP,
        open_price DECIMAL(15, 4) NOT NULL,
        high_price DECIMAL(15, 4) NOT NULL,
        low_price DECIMAL(15, 4) NOT NULL,
        close_price DECIMAL(15, 4) NOT NULL,
        volume BIGINT NOT NULL,
        PRIMARY KEY (company_symbol, date_time),
        FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        );
    """)
    
    tables.append('intraday_stock_prices')
    print("intraday_stock_prices table created!")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sma_indicators (
        company_symbol varchar(10),
        date_time TIMESTAMP,
        sma_value DECIMAL(15, 4) NOT NULL,
        PRIMARY KEY (company_symbol, date_time),
        FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        );
    """)

    tables.append('sma_indicators')
    print("sma_indicators table created!")
    return tables

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_log_duckdb.log"),
        logging.StreamHandler()
    ]
)

def execute_query(query, values = None):
    logging.info(f"Executing query: {query} with values: {values}")
    # Simulate query execution
    try:
        # Here you would have your database execution logic
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        logging.info("Query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")


def check_last_date(symbol, table):
    if table == "daily_stock_prices":
        query = f"SELECT MAX(date) FROM {table} WHERE company_symbol = ?"
    else:
        query = f"SELECT MAX(date_time) FROM {table} WHERE company_symbol = ?"

    try:
        execute_query(query, values=(symbol,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    except Exception as e:
        logging.error(f"Error fetching last date for {symbol}: {e}")
        return None
    
def insert_daily_stock_prices(symbol, date, values):
    query = f"""
        INSERT OR IGNORE INTO daily_stock_prices (company_symbol, date, open_price, high_price, low_price, close_price, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_query(
            query, 
            values=(
                symbol, 
                date,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume']),
            )
        )
    except Exception as e:
        logging.error(f"Error inserting daily stock prices for {symbol}: {e}")

def update_daily_stock_prices(symbol, time_series_data, table):

    try:
        last_date = check_last_date(symbol, table)

        if last_date is not None:
            for date, values in time_series_data.items():
                if datetime.strptime(date, '%Y-%m-%d').date() > last_date:
                    insert_daily_stock_prices(symbol, date, values)
                else:
                    break
        else:
            for date, values in time_series_data.items():
                insert_daily_stock_prices(symbol, date, values)
    except Exception as e:
        logging.error(f"Error updating daily stock prices for {symbol}: {e}")


def insert_intraday_stock_prices(symbol, date_time, values):
    query = f"""
        INSERT OR IGNORE INTO intraday_stock_prices (company_symbol, date_time, open_price, high_price, low_price, close_price, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_query(
            query, 
            values=(
                symbol, 
                date_time,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume']),
            )
        )
    except Exception as e:
        logging.error(f"Error inserting intraday stock prices for {symbol}: {e}")

def update_intraday_stock_prices(symbol, time_series_data, table):
    
    try:
        last_date = check_last_date(symbol, table)

        if last_date is not None:
            for date_time, values in time_series_data.items():
                if datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') > last_date:
                    insert_intraday_stock_prices(symbol, date_time, values)
                else:
                    break
        else:
            for date_time, values in time_series_data.items():
                insert_intraday_stock_prices(symbol, date_time, values)
    except Exception as e:
        logging.error(f"Error updating intraday stock prices for {symbol}: {e}")

def insert_sma_indicators(symbol, date, values):
    query = f"""
        INSERT OR IGNORE INTO sma_indicators (company_symbol, date_time, sma_value)
        VALUES (?, ?, ?)
    """
    try:
        execute_query(
            query, 
            values=(
                symbol, 
                date,
                float(values['SMA']),
            )
        )
    except Exception as e:
        logging.error(f"Error inserting sma indicators for {symbol}: {e}")

def update_sma_indicators(symbol, time_series_data, table):
    
    try:
        last_date = check_last_date(symbol, table)

        if last_date is not None:
            for date, values in time_series_data.items():
                if datetime.strptime(date, '%Y-%m-%d %H:%M:%S') > last_date:
                    insert_sma_indicators(symbol, date, values)
                else:
                    break
        else:
            for date, values in time_series_data.items():
                insert_sma_indicators(symbol, date, values)
    except Exception as e:
        logging.error(f"Error updating sma indicators for {symbol}: {e}")

def insert_company(symbol):
    query = f"""
        INSERT OR IGNORE INTO companies (company_symbol) VALUES (?)
    """
    try:
        execute_query(query, values=(symbol,))
    except Exception as e:
        logging.error(f"Error inserting company {symbol}: {e}")

def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

def fetch_daily_stock_prices(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    return fetch_data_from_api(url)

def fetch_intraday_stock_prices(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={api_key}"
    return fetch_data_from_api(url)

def fetch_sma_indicators(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=SMA&symbol={symbol}&interval=60min&time_period=200&series_type=close&apikey={api_key}"
    return fetch_data_from_api(url)

def main():
    # Initialize connection
    init_connection()
    
    # Get API key from environment
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
    if not api_key:
        logging.error("ALPHA_VANTAGE_API_KEY not found in environment variables")
        return
    
    # Create tables
    tables = create_tables()
    logging.info(f"Tables created: {tables}")
    
    # List of stock symbols to fetch
    symbols = ['AAPL', 'IBM', 'MSFT', 'GOOGL']
    
    for symbol in symbols:
        logging.info(f"Processing symbol: {symbol}")
        
        # Insert company
        insert_company(symbol)
        
        # Fetch and update daily stock prices
        daily_data = fetch_daily_stock_prices(symbol, api_key)
        if daily_data and 'Time Series (Daily)' in daily_data:
            time_series = daily_data['Time Series (Daily)']
            update_daily_stock_prices(symbol, time_series, 'daily_stock_prices')
            logging.info(f"Updated daily stock prices for {symbol}")
        else:
            logging.warning(f"No daily stock price data found for {symbol}")
        
        # Fetch and update intraday stock prices
        intraday_data = fetch_intraday_stock_prices(symbol, api_key)
        if intraday_data and 'Time Series (5min)' in intraday_data:
            time_series = intraday_data['Time Series (5min)']
            update_intraday_stock_prices(symbol, time_series, 'intraday_stock_prices')
            logging.info(f"Updated intraday stock prices for {symbol}")
        else:
            logging.warning(f"No intraday stock price data found for {symbol}")
        
        # Fetch and update SMA indicators
        sma_data = fetch_sma_indicators(symbol, api_key)
        if sma_data and 'Technical Analysis: SMA' in sma_data:
            technical_analysis = sma_data['Technical Analysis: SMA']
            update_sma_indicators(symbol, technical_analysis, 'sma_indicators')
            logging.info(f"Updated SMA indicators for {symbol}")
        else:
            logging.warning(f"No SMA data found for {symbol}")
    
    # Close connection
    if conn:
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
