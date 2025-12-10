import logging
import requests
import json
import psycopg2
from psycopg2 import pool
from datetime import datetime
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import threading

load_dotenv()

# Thread-local storage for database connections
thread_local = threading.local()

# Global connection pool
db_pool: Optional[pool.ThreadedConnectionPool] = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    sslmode: str = "require"
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        return cls(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            port=int(os.environ.get("DB_PORT", 5432)),
            sslmode=os.environ.get("DB_SSLMODE", "require"),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port,
            "sslmode": self.sslmode,
        }


@dataclass
class APIConfig:
    base_url: str = "https://www.alphavantage.co/query"
    api_key: str = ""
    
    @classmethod
    def from_env(cls) -> 'APIConfig':
        return cls(
            api_key=os.environ.get("ALPHA_VANTAGE_API_KEY", "")
        )


def init_connection_pool(db_config: DatabaseConfig, min_conn: int = 2, max_conn: int = 20):
    """Initialize the database connection pool."""
    global db_pool
    db_pool = pool.ThreadedConnectionPool(
        min_conn,
        max_conn,
        **db_config.to_dict()
    )
    logger.info(f"Connection pool initialized with {min_conn}-{max_conn} connections")


def get_connection():
    """Get a connection from the pool for the current thread."""
    if db_pool is None:
        raise RuntimeError("Connection pool not initialized")
    return db_pool.getconn()


def release_connection(conn):
    """Release a connection back to the pool."""
    if db_pool is not None and conn is not None:
        db_pool.putconn(conn)


def close_connection_pool():
    """Close all connections in the pool."""
    global db_pool
    if db_pool is not None:
        db_pool.closeall()
        db_pool = None
        logger.info("Connection pool closed")


def execute_query(query: str, values: tuple = None, fetch: bool = False):
    """Execute a query using a connection from the pool."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        
        result = None
        if fetch:
            result = cursor.fetchone()
        
        conn.commit()
        logger.debug(f"Query executed successfully: {query[:50]}...")
        return result
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error executing query: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


def execute_batch(query: str, values_list: List[tuple]):
    """Execute batch insert using executemany for better performance."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.executemany(query, values_list)
        conn.commit()
        logger.info(f"Batch insert completed: {len(values_list)} rows")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error executing batch: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


def create_tables():
    """Create all required tables."""
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS companies (
            company_symbol VARCHAR(10) PRIMARY KEY
        )""",
        """CREATE TABLE IF NOT EXISTS daily_stock_prices (
            company_symbol VARCHAR(10),
            date DATE,
            open_price DECIMAL(15, 4) NOT NULL,
            high_price DECIMAL(15, 4) NOT NULL,
            low_price DECIMAL(15, 4) NOT NULL,
            close_price DECIMAL(15, 4) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (company_symbol, date),
            FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        )""",
        """CREATE TABLE IF NOT EXISTS intraday_stock_prices (
            company_symbol VARCHAR(10),
            date_time TIMESTAMP,
            open_price DECIMAL(15, 4) NOT NULL,
            high_price DECIMAL(15, 4) NOT NULL,
            low_price DECIMAL(15, 4) NOT NULL,
            close_price DECIMAL(15, 4) NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (company_symbol, date_time),
            FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        )""",
        """CREATE TABLE IF NOT EXISTS sma_indicators (
            company_symbol VARCHAR(10),
            date_time TIMESTAMP,
            sma_value DECIMAL(15, 4) NOT NULL,
            PRIMARY KEY (company_symbol, date_time),
            FOREIGN KEY (company_symbol) REFERENCES companies(company_symbol)
        )"""
    ]
    
    for sql in tables_sql:
        execute_query(sql)
    logger.info("All tables created successfully")


def check_last_date(symbol: str, table: str) -> Optional[datetime]:
    """Check the last date/datetime for a symbol in a table."""
    if table == "daily_stock_prices":
        query = "SELECT MAX(date) FROM daily_stock_prices WHERE company_symbol = %s"
    else:
        query = "SELECT MAX(date_time) FROM {} WHERE company_symbol = %s".format(table)
    
    try:
        result = execute_query(query, values=(symbol,), fetch=True)
        return result[0] if result and result[0] else None
    except Exception as e:
        logger.error(f"Error fetching last date for {symbol}: {e}")
        return None


def ensure_company_exists(symbol: str):
    """Ensure a company exists in the companies table."""
    try:
        query = "INSERT INTO companies (company_symbol) VALUES (%s) ON CONFLICT DO NOTHING"
        execute_query(query, values=(symbol,))
    except Exception as e:
        logger.error(f"Error ensuring company existence for {symbol}: {e}")


def fetch_api_data(api_config: APIConfig, endpoint: str, symbol: str) -> Dict[str, Any]:
    """Fetch data from Alpha Vantage API."""
    params = {
        'function': endpoint,
        'symbol': symbol,
        'apikey': api_config.api_key
    }
    
    if endpoint == 'TIME_SERIES_INTRADAY':
        params['interval'] = '5min'
    elif endpoint == 'SMA':
        params['interval'] = '60min'
        params['time_period'] = '200'
        params['series_type'] = 'close'
    
    try:
        response = requests.get(api_config.base_url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed for {symbol} - {endpoint}: {e}")
        return {}


def process_daily_stock_prices(symbol: str, time_series_data: Dict[str, Any]):
    """Process and insert daily stock prices."""
    if not time_series_data:
        return
    
    last_date = check_last_date(symbol, "daily_stock_prices")
    
    values_list = []
    for date_str, values in time_series_data.items():
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            if last_date is not None and date_obj <= last_date:
                continue
            
            values_list.append((
                symbol,
                date_str,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume']),
            ))
        except (KeyError, ValueError) as e:
            logger.warning(f"Skipping invalid data for {symbol} on {date_str}: {e}")
    
    if values_list:
        query = """
            INSERT INTO daily_stock_prices 
            (company_symbol, date, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_symbol, date) DO NOTHING
        """
        execute_batch(query, values_list)
        logger.info(f"Inserted {len(values_list)} daily prices for {symbol}")


def process_intraday_stock_prices(symbol: str, time_series_data: Dict[str, Any]):
    """Process and insert intraday stock prices."""
    if not time_series_data:
        return
    
    last_date = check_last_date(symbol, "intraday_stock_prices")
    
    values_list = []
    for date_time_str, values in time_series_data.items():
        try:
            date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
            if last_date is not None and date_time_obj <= last_date:
                continue
            
            values_list.append((
                symbol,
                date_time_str,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume']),
            ))
        except (KeyError, ValueError) as e:
            logger.warning(f"Skipping invalid data for {symbol} at {date_time_str}: {e}")
    
    if values_list:
        query = """
            INSERT INTO intraday_stock_prices 
            (company_symbol, date_time, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_symbol, date_time) DO NOTHING
        """
        execute_batch(query, values_list)
        logger.info(f"Inserted {len(values_list)} intraday prices for {symbol}")


def process_sma_indicators(symbol: str, indicator_data: Dict[str, Any]):
    """Process and insert SMA indicators."""
    if not indicator_data:
        return
    
    last_date = check_last_date(symbol, "sma_indicators")
    
    values_list = []
    for date_time_str, values in indicator_data.items():
        try:
            # SMA data can have different date formats
            try:
                date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
            
            if last_date is not None and date_time_obj <= last_date:
                continue
            
            values_list.append((
                symbol,
                date_time_str,
                float(values['SMA']),
            ))
        except (KeyError, ValueError) as e:
            logger.warning(f"Skipping invalid SMA data for {symbol} at {date_time_str}: {e}")
    
    if values_list:
        query = """
            INSERT INTO sma_indicators (company_symbol, date_time, sma_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (company_symbol, date_time) DO NOTHING
        """
        execute_batch(query, values_list)
        logger.info(f"Inserted {len(values_list)} SMA indicators for {symbol}")


def process_symbol_endpoint(api_config: APIConfig, symbol: str, endpoint: str) -> Dict[str, Any]:
    """Process a single symbol-endpoint combination."""
    result = {
        'symbol': symbol,
        'endpoint': endpoint,
        'success': False,
        'message': ''
    }
    
    try:
        logger.info(f"Fetching {endpoint} data for {symbol}")
        json_data = fetch_api_data(api_config, endpoint, symbol)
        
        if 'Error Message' in json_data:
            result['message'] = json_data['Error Message']
            return result
        
        if 'Note' in json_data:  # API rate limit
            result['message'] = json_data['Note']
            return result
        
        if endpoint == 'TIME_SERIES_DAILY':
            time_series_data = json_data.get('Time Series (Daily)', {})
            process_daily_stock_prices(symbol, time_series_data)
        elif endpoint == 'TIME_SERIES_INTRADAY':
            time_series_data = json_data.get('Time Series (5min)', {})
            process_intraday_stock_prices(symbol, time_series_data)
        elif endpoint == 'SMA':
            indicator_data = json_data.get('Technical Analysis: SMA', {})
            process_sma_indicators(symbol, indicator_data)
        
        result['success'] = True
        result['message'] = 'Processed successfully'
        
    except Exception as e:
        result['message'] = str(e)
        logger.error(f"Error processing {symbol} - {endpoint}: {e}")
    
    return result


def run_parallel_etl(
    symbols: List[str],
    endpoints: List[str],
    api_config: APIConfig,
    max_workers: int = 5
):
    """Run ETL process in parallel for all symbol-endpoint combinations."""
    
    # First ensure all companies exist (can be done in parallel)
    logger.info("Ensuring all companies exist in database...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(ensure_company_exists, symbol) for symbol in symbols]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error ensuring company existence: {e}")
    
    # Create all tasks (symbol-endpoint combinations)
    tasks = [(symbol, endpoint) for symbol in symbols for endpoint in endpoints]
    
    logger.info(f"Starting parallel ETL for {len(tasks)} tasks with {max_workers} workers")
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(process_symbol_endpoint, api_config, symbol, endpoint): (symbol, endpoint)
            for symbol, endpoint in tasks
        }
        
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
                status = "✓" if result['success'] else "✗"
                logger.info(f"{status} {result['symbol']} - {result['endpoint']}: {result['message']}")
            except Exception as e:
                logger.error(f"Task {task} generated an exception: {e}")
                results.append({
                    'symbol': task[0],
                    'endpoint': task[1],
                    'success': False,
                    'message': str(e)
                })
    
    # Summary
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    logger.info(f"ETL completed: {successful} successful, {failed} failed out of {len(results)} tasks")
    
    return results


def main():
    """Main entry point."""
    # Configuration
    db_config = DatabaseConfig.from_env()
    api_config = APIConfig.from_env()
    
    symbols = ['AAPL', 'IBM', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'NFLX', 'INTC']
    endpoints = ['TIME_SERIES_DAILY', 'TIME_SERIES_INTRADAY', 'SMA']
    
    # Number of parallel workers (be mindful of API rate limits)
    # Alpha Vantage free tier: 5 calls/minute, 500 calls/day
    max_workers = 3
    
    try:
        # Initialize connection pool
        init_connection_pool(db_config, min_conn=2, max_conn=max_workers + 2)
        
        # Create tables
        create_tables()
        
        # Run parallel ETL
        results = run_parallel_etl(symbols, endpoints, api_config, max_workers=max_workers)
        
        # Print summary
        print("\n" + "="*60)
        print("ETL SUMMARY")
        print("="*60)
        for result in results:
            status = "SUCCESS" if result['success'] else "FAILED"
            print(f"{result['symbol']:6} | {result['endpoint']:20} | {status}")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise
    finally:
        close_connection_pool()


if __name__ == "__main__":
    main()
