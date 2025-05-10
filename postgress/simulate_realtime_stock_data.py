import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import psycopg2
from psycopg2.extras import execute_values

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA']
POSTGRES_CONN = {
    'dbname': 'stock_db',
    'user': 'stock_user',
    'password': 'stock_pass',
    'host': 'localhost',
    'port': '5432'
}
UPDATE_INTERVAL_SECONDS = 60  # Update every 60 seconds
HISTORICAL_DAYS = 30  # Seed 30 days of historical data

def generate_stock_data(ticker, start_date):
    logger.info(f"Generating data for {ticker} on {start_date.date()}...")
    np.random.seed(42 + hash(ticker) % 1000 + start_date.day)
    base_price = 100 + np.random.uniform(50, 200)
    volatility = 0.02
    
    price = base_price * (1 + np.random.normal(0, volatility))
    volume = int(np.random.uniform(10000, 100000))
    
    data = {
        'symbol': [ticker],
        'timestamp': [start_date],
        'open': [price],
        'high': [price * (1 + np.random.uniform(0, 0.01))],
        'low': [price * (1 - np.random.uniform(0, 0.01))],
        'close': [price * (1 + np.random.normal(0, 0.005))],
        'volume': [volume]
    }
    return pd.DataFrame(data)

def aggregate_to_daily(df):
    df['date'] = df['timestamp'].dt.date
    daily_df = df.groupby(['symbol', 'date']).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    return daily_df

def save_to_postgres(df, conn):
    try:
        with conn.cursor() as cur:
            data_tuples = [
                (
                    row['symbol'],
                    row['date'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume']
                )
                for _, row in df.iterrows()
            ]
            execute_values(
                cur,
                """
                INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                data_tuples
            )
            conn.commit()
            logger.info(f"Saved {len(data_tuples)} daily records to PostgreSQL")
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL: {e}")
        conn.rollback()

def main():
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        logger.info("Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return

    try:
        # Clear existing data to start fresh
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_data;")
            conn.commit()
            logger.info("Cleared stock_data table")

        # Seed historical data for the past 30 days (April 11, 2025 to May 10, 2025)
        historical_start_date = datetime(2025, 4, 11)  # Start from April 11, 2025
        for day in range(HISTORICAL_DAYS):
            sim_date = historical_start_date + timedelta(days=day)
            all_data = []
            for ticker in TICKERS:
                df = generate_stock_data(ticker, sim_date)
                all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data)
                daily_df = aggregate_to_daily(combined_df)
                save_to_postgres(daily_df, conn)
                logger.info(f"Inserted historical data for {sim_date.date()}")

        # Real-time simulation: Add new daily data starting from May 11, 2025
        day_counter = 0
        real_time_start_date = datetime(2025, 5, 11)  # Start real-time from May 11, 2025
        while True:
            sim_date = real_time_start_date + timedelta(days=day_counter)
            all_data = []
            for ticker in TICKERS:
                df = generate_stock_data(ticker, sim_date)
                all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data)
                daily_df = aggregate_to_daily(combined_df)
                save_to_postgres(daily_df, conn)
                logger.info(f"Inserted real-time data for {sim_date.date()}")
            
            day_counter += 1
            time.sleep(UPDATE_INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        logger.info("Stopping real-time simulation")
    finally:
        conn.close()
        logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    main()