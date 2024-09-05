import os
import schedule
import sqlite3
import pandas as pd
import json
from dotenv import load_dotenv
from utils.util import get_secret, delta_append
from config.logger_config import logger
import time 

load_dotenv()

def transform_and_push_to_snowflake():
    logger.info("Starting daily snowflake push")

    # Connect to SQLite database
    conn = sqlite3.connect('opcodes.db')

    # Fetch unprocessed data from SQLite
    query = '''
    SELECT BLOCK_NUMBER, TRANSACTION_HASH, OPCODE, GAS, GAS_COST, STATE_GROWTH_COUNT
    FROM raw_opcodes
    WHERE PROCESSED = 'N'
    '''
    df = pd.read_sql_query(query, conn)

    if df.empty:
        logger.info("No new data to process")
        conn.close()
        return
    
    # Get the smallest and largest unprocessed block numbers
    min_block = df['BLOCK_NUMBER'].min()
    max_block = df['BLOCK_NUMBER'].max()

    # First aggregation level: Aggregate by TRANSACTION_HASH to get MAX_GAS and MIN_GAS
    start_time = time.time()
    transaction_agg = df.groupby(['BLOCK_NUMBER', 'TRANSACTION_HASH']).agg(
        MAX_GAS=('GAS', 'max'),
        MIN_GAS=('GAS', 'min')
    ).reset_index()

    # Second aggregation level: Aggregate by TRANSACTION_HASH and OPCODE
    opcode_agg = df.groupby(['BLOCK_NUMBER', 'TRANSACTION_HASH', 'OPCODE']).agg(
        OPCODE_COUNT=('OPCODE', 'size'),
        SUM_GAS_COST=('GAS_COST', 'sum'),
        STATE_GROWTH_COUNT=('STATE_GROWTH_COUNT', 'sum')
    ).reset_index()

    # Merge the two aggregation results
    merged_df = pd.merge(transaction_agg, opcode_agg, on=['BLOCK_NUMBER', 'TRANSACTION_HASH'])

    end_time = time.time()
    logger.info(f"Time taken to perform transformations on blocks {min_block} to {max_block}: {end_time - start_time:.2f} seconds")


    # Push to Snowflake
    secret = get_secret(user='notnotsez-peter')
    table_name = os.getenv("TABLE_NAME")

    try:
        delta_append(secret, 'STAGING', table_name, merged_df)
        logger.info(f"Successfully pushed {len(merged_df)} rows to Snowflake")

        # Mark processed rows in SQLite
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE raw_opcodes
        SET processed = 'Y'
        WHERE processed = 'N'
        ''')
        conn.commit()
        logger.info(f"Marked {cursor.rowcount} rows as processed in SQLite")

    except Exception as e:
        logger.error(f"Error pushing data to Snowflake: {e}")
        conn.rollback()

    finally:
        conn.close()

    logger.info("Daily Snowflake push completed")


# Schedule the job to run daily at 5:00 PM machine time
schedule.every().day.at("17:00").do(transform_and_push_to_snowflake)

if __name__ == "__main__":
    logger.info("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Sleep for 60 seconds before checking schedule again