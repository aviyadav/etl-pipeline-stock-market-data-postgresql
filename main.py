import logging
import datetime
import requests
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

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
        volume INT NOT NULL,
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
        volume INT NOT NULL,
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
        PRIMARY KEY (company_symbol, date),
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
        logging.FileHandler("etl_log.log"),
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
    if table == "daily_stock_pries":
        query = f"SELECT MAX(date) FROM {table} WHERE company_symbol = '{symbol}'"
    else:
        query = f"SELECT MAX(date_time) FROM {table} WHERE company_symbol = '{symbol}'"

    try:
        execute_query(query, values=(symbol,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    except Exception as e:
        logging.error(f"Error fetching last date for {symbol}: {e}")
        return None
    
def insert_daily_stock_prices(symbol, date, values):
    query = f"""
        INSERT INTO daily_stock_prices (company_symbol, date, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
        INSERT INTO intraday_stock_prices (company_symbol, date_time, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
        INSERT INTO sma_indicators (company_symbol, date_time, sma_value)
        VALUES (%s, %s, %s)
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
        logging.error(f"Error inserting SMA indicators for {symbol}: {e}")

def update_sma_indicators(symbol, indicator_data, table):
    
    try:
        last_date = check_last_date(symbol, table)

        if last_date is not None:
            for date, values in indicator_data.items():
                if datetime.strptime(date, '%Y-%m-%d %H:%M:%S') > last_date:
                    insert_sma_indicators(symbol, date, values)
                else:
                    break
        else:
            for date, values in indicator_data.items():
                insert_sma_indicators(symbol, date, values)
    except Exception as e:
        logging.error(f"Error updating SMA indicators for {symbol}: {e}")


def update(symbols, endpoints):
    print(symbols, endpoints)
    try:
        for endpoint in endpoints:
            for symbol in symbols:
                if endpoint == 'TIME_SERIES_DAILY':
                    url = f'{base_url}?function={endpoint}&symbol={symbol}&apikey={api_key}'
                    response = requests.get(url)
                    data = response.text
                    json_data = json.loads(data)
                    time_series_data = json_data.get('Time Series (Daily)', {})
                    update_daily_stock_prices(symbol, time_series_data, "daily_stock_prices")
                elif endpoint == 'TIME_SERIES_INTRADAY':
                    url = f'{base_url}?function={endpoint}&symbol={symbol}&interval=5min&apikey={api_key}'
                    response = requests.get(url)
                    data = response.text
                    json_data = json.loads(data)
                    time_series_data = json_data.get('Time Series (5min)', {})
                    update_intraday_stock_prices(symbol, time_series_data, "intraday_stock_prices")
                elif endpoint == 'SMA':
                    url = f'{base_url}?function={endpoint}&symbol={symbol}&interval=60min&time_period=200&series_type=close&apikey={api_key}'
                    response = requests.get(url)
                    data = response.text
                    json_data = json.loads(data)
                    time_series_data = json_data.get('Technical Analysis: SMA', {})
                    update_sma_indicators(symbol, time_series_data, "sma_indicators")
                    
    except:  
        logging.error(f"Error in the update function: {e}")


def check_company_existence(symbol):
    try:
        query = "SELECT COUNT(*) FROM companies WHERE company_symbol = %s"
        cursor.execute(query, (symbol,))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logging.error(f"Error checking company existence: {e}")
        return False

def insert_company(symbol):
    try:
        query = "INSERT INTO companies (company_symbol) VALUES (%s)"
        cursor.execute(query, (symbol,))
        logging.info(f"Inserted company {symbol} into companies table.")
    except Exception as e:
        logging.error(f"Error inserting company {symbol}: {e}")

def ensure_company_existence(symbol):
    if not check_company_existence(symbol):
        insert_company(symbol)

if __name__ == "__main__":

    
    DATABASE_CONFIG = {
        "host": os.environ.get("DB_HOST"),
        "database": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "port": os.environ.get("DB_PORT", 5432),
        "sslmode": "require",
        "channel_binding": "require"
    }

    base_url = "https://www.alphavantage.co/query"
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")

    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    conn.autocommit = True

    try:
        symbols = ['AAPL', 'IBM', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'NFLX', 'INTC']
        endpoints = ['TIME_SERIES_DAILY', 'TIME_SERIES_INTRADAY', 'SMA']

        # Assuming you have a function create_tables() defined
        # create_tables()

        for symbol in symbols:
            ensure_company_existence(symbol)


        update(symbols, endpoints)

    except Exception as e:
        logging.error(f"Error in main: {e}")