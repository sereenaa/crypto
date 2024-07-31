
from dotenv import load_dotenv
import os
import time
from web3 import Web3
import argparse
import platform
import logging
from datetime import datetime

from utils.util import *
from utils.cryo_fetch import *

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
rpc = os.getenv("ETH_RPC")

# Snowflake connection details
secret = get_secret(user='notnotsez')

# Connect to the blockchain RPC endpoint
rpc_url = 'https://rpc.apex.proofofplay.com'
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))

# Configure logging if running on Windows
if platform.system() == 'Windows':
    logs_folder = 'logs'
    os.makedirs(logs_folder, exist_ok=True)
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = os.path.join(logs_folder, f'log_{current_datetime}.log')

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)

def fetch_and_push_data(data_type, secret, rpc, start_block, end_block):
    if data_type == 'transactions':
        fetch_and_push_transactions_with_cryo(secret, rpc, start_block, end_block)
    elif data_type == 'logs':
        fetch_and_push_logs_with_cryo(secret, rpc, start_block, end_block)
    else:
        raise ValueError("Invalid data type. Must be either 'transactions' or 'logs'.")

# Main function to fetch and store recent blocks
def main(data_type):
    try:
        start_time = time.time()

        latest_block = web3.eth.block_number

        # Fetch the latest timestamps and block numbers from Snowflake
        try:
            latest_block_number = fetch_latest_block_number(secret, data_type.upper())
        except Exception as e:
            error_msg = f"Error fetching latest block numbers from Snowflake: {e}"
            print(error_msg)
            if platform.system() == 'Windows':
                logger.error(error_msg)
            return

        # Determine the start block based on the latest block number from Snowflake
        start_block = latest_block_number + 1 if latest_block_number else latest_block
        batch_size = 100  # Set the batch size to 100 blocks

        # Run fetch data loop
        batch_start = start_block
        while batch_start < latest_block:
            batch_end = min(batch_start + batch_size, latest_block)
            try:
                fetch_and_push_data(data_type, secret, rpc, batch_start, batch_end)
            except Exception as e:
                error_msg = f"Error fetching or pushing data for blocks {batch_start} to {batch_end}: {e}"
                print(error_msg)
                if platform.system() == 'Windows':
                    logger.error(error_msg)
            batch_start += batch_size

        end_time = time.time()
        success_msg = f"{data_type.capitalize()} data for blocks {format_number_with_commas(start_block)} to {format_number_with_commas(latest_block)} written to Snowflake in {end_time - start_time:.2f} seconds."
        print(success_msg)
        if platform.system() == 'Windows':
            logger.info(success_msg)
    except Exception as e:
        error_msg = f"Unexpected error in main function: {e}"
        print(error_msg)
        if platform.system() == 'Windows':
            logger.error(error_msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch and store blockchain data.')
    parser.add_argument('data_type', choices=['transactions', 'logs'], help='The type of data to fetch and store (transactions or logs).')
    args = parser.parse_args()
    main(args.data_type)
