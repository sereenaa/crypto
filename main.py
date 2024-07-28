
import concurrent.futures
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
    latest_tx_block = fetch_latest_block_number(secret, 'TRANSACTIONS')
    latest_log_block = fetch_latest_block_number(secret, 'LOGS')
    latest_trace_block = fetch_latest_block_number(secret, 'TRACES')

    # Determine the start block based on the latest block number from Snowflake
    start_block_tx = latest_tx_block + 1 if latest_tx_block else latest_block
    start_block_log = latest_log_block + 1 if latest_log_block else latest_block
    start_block_trace = latest_trace_block + 1 if latest_trace_block else latest_block

    batch_size = 100  # Set the batch size to 100 blocks

    # Fetch and store data in batches
    # batch_start = start_block_tx
    # while batch_start < latest_block:
    #     batch_end = min(batch_start + batch_size, latest_block)
    #     fetch_and_push_transactions_with_cryo(secret, rpc, batch_start, batch_end)
    #     batch_start += batch_size

    # Run all 3 fetch data loops simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        batch_start_tx = start_block_tx
        batch_start_log = start_block_log
        batch_start_trace = start_block_trace
        while batch_start_tx < latest_block or batch_start_log < latest_block:
            futures = []
            if batch_start_tx < latest_block:
                batch_end_tx = min(batch_start_tx + batch_size, latest_block)
                futures.append(executor.submit(fetch_and_push_transactions_with_cryo, secret, rpc, batch_start_tx, batch_end_tx))
                batch_start_tx += batch_size
            if batch_start_log < latest_block:
                batch_end_log = min(batch_start_log + batch_size, latest_block)
                futures.append(executor.submit(fetch_and_push_logs_with_cryo, secret, rpc, batch_start_log, batch_end_log))
                batch_start_log += batch_size
            # if batch_start_trace < latest_block:
            #     batch_end_trace = min(batch_start_trace + batch_size, latest_block)
            #     futures.append(executor.submit(fetch_and_push_traces_with_cryo, secret, rpc, batch_start_trace, batch_end_trace))
            #     batch_start_trace += batch_size

            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                future.result()

    end_time = time.time()
    print(f"Transaction and log data for blocks {format_number_with_commas(start_block_tx)} to {format_number_with_commas(latest_block)} written to Snowflake in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()