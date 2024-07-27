

from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
import time
from web3 import Web3

from get_secret import get_secret
from util import *

# Load environment variables from .env file
load_dotenv()

# Retrieve the Ethereum RPC URL from environment variables
rpc = os.getenv("ETH_RPC")

# Snowflake connection details
secret = get_secret(user='notnotsez')

# Connect to the blockchain RPC endpoint
rpc_url = 'https://rpc.apex.proofofplay.com'
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))


# Main function to fetch and store recent blocks
def main():
    start_time = time.time()

    latest_block = web3.eth.block_number

    # Fetch the latest timestamps and block numbers from Snowflake
    latest_tx_timestamp, latest_tx_block = fetch_latest_timestamp_and_block_number(secret, 'TRANSACTIONS')

    # Determine the start block based on the latest block number from Snowflake
    start_block_tx = latest_tx_block + 1 if latest_tx_block else latest_block

    batch_size = 100  # Set the batch size to 100 blocks

    # Fetch and store data in batches
    batch_start = start_block_tx
    while batch_start <= latest_block:
      batch_end = min(batch_start + batch_size, latest_block)
      fetch_and_push_transactions_with_cryo(secret, rpc, batch_start, batch_end)
      batch_start += batch_size

    end_time = time.time()
    print(f"Transaction data for blocks {format_number_with_commas(start_block_tx)} to {format_number_with_commas(latest_block)} written to Snowflake in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()