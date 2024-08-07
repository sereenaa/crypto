
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
if not rpc_url:
    raise ValueError("RPC_URL is not set. Please check your .env file.")

# Snowflake connection details
secret = get_secret(user='notnotsez-peter')

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
def main(run_strategy, start_block=None, end_block=None, block_range=200):
    try:
        start_time = time.time()

        if run_strategy == 'default':
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

        elif run_strategy == 'historical':
            print('Run strategy: historical')
            range_end_block = start_block + block_range
            print('end block:', str(end_block))
            print('start block:', str(start_block))

        range_start_block = start_block
        # while block_number < latest_block:
        #     try:
        #         fetch_and_push_raw_opcodes(secret, rpc_url, block_number)
        #     except Exception as e:
        #         error_msg = f"Error fetching or pushing data for block {block_number}: {e}"
        #         print(error_msg)
        #         if platform.system() == 'Windows':
        #             logger.error(error_msg)
        #     block_number += 1

        while range_end_block <= end_block:
            try:
                fetch_and_push_raw_opcodes_for_block_range(secret, 'OPCODES_TEST', rpc_url, range_start_block, range_end_block)
            except Exception as e:
                error_msg = f"Error fetching or pushing data for block range {format_number_with_commas(range_start_block)}-{format_number_with_commas(range_end_block)}: {e}"
                print(error_msg)
                if platform.system() == 'Windows':
                    logger.error(error_msg)
            range_start_block += block_range
            range_end_block += block_range

        end_time = time.time()
        success_msg = f"OPCODES data for blocks {format_number_with_commas(start_block)} to {format_number_with_commas(end_block)} written to Snowflake in {end_time - start_time:.2f} seconds."
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
    parser.add_argument('run_strategy', choices=['default', 'historical'], help='The run strategy for this run. Choose default to load data for a daily load, historical for chunking historical blocks into batches for faster processing and multiple invocations of this script.')
    parser.add_argument('start_block', type=int, help='The start block for this run.')
    parser.add_argument('end_block', type=int, help='The end block for this run.')
    args = parser.parse_args()
    main(args.run_strategy, args.start_block, args.end_block)