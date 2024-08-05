
from dotenv import load_dotenv
import os
import time
from web3 import Web3
import argparse
import platform
import logging
from datetime import datetime

from utils.util import *
from utils.process_data import *

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
rpc_url = os.getenv("RPC_URL")

# Snowflake connection details
secret = get_secret(user='notnotsez')

# Connect to the blockchain RPC endpoint
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))

# Configure logging if running on Windows
if platform.system() == 'Windows':
    logs_folder = 'logs'
    os.makedirs(logs_folder, exist_ok=True)
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = os.path.join(logs_folder, f'log_{current_datetime}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)


# Main function to fetch and store recent blocks
def main():
    try:
        start_time = time.time()

        latest_block = web3.eth.block_number

        # Fetch the latest timestamps and block numbers from Snowflake
        try:
            latest_block_number = fetch_latest_block_number(secret, 'STAGING', 'OPCODES')
        except Exception as e:
            error_msg = f"Error fetching latest block numbers from Snowflake: {e}"
            print(error_msg)
            if platform.system() == 'Windows':
                logger.error(error_msg)
            return

        # Determine the start block based on the latest block number from Snowflake
        start_block = latest_block_number + 1 if latest_block_number else latest_block
        
        block_number = start_block
        while block_number < latest_block:
            try:
                fetch_and_push_raw_opcodes(secret, rpc_url, block_number)
            except Exception as e:
                error_msg = f"Error fetching or pushing data for block {block_number}: {e}"
                print(error_msg)
                if platform.system() == 'Windows':
                    logger.error(error_msg)
            block_number += 1

        end_time = time.time()
        success_msg = f"OPCODES data for blocks {format_number_with_commas(start_block)} to {format_number_with_commas(latest_block)} written to Snowflake in {end_time - start_time:.2f} seconds."
        print(success_msg)
        if platform.system() == 'Windows':
            logger.info(success_msg)
    except Exception as e:
        error_msg = f"Unexpected error in main function: {e}"
        print(error_msg)
        if platform.system() == 'Windows':
            logger.error(error_msg)

if __name__ == "__main__":
    main()