
import time
from web3 import Web3
from get_secret import get_secret
from concurrent.futures import ThreadPoolExecutor, as_completed

from util import *

# Snowflake connection details
secret = get_secret(user='notnotsez')

# Connect to the blockchain RPC endpoint
rpc_url = 'https://rpc.apex.proofofplay.com'
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))


def fetch_and_store_transactions(start_block, end_block, batch_size, max_workers=10):
    data = []
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_transactions, web3, block_num) for block_num in range(start_block, end_block + 1)]
        for future in as_completed(futures):
            data.extend(future.result())
            if len(data) >= batch_size:
                delta_append(secret, 'TRANSACTIONS', data, 'TX_HASH')
                end_time = time.time()
                print(f"Time taken for batch of {batch_size} transactions: {end_time - start_time:.2f} seconds")
                start_time = time.time()
                data = []

    if data:
        delta_append(secret, 'TRANSACTIONS', data, 'TX_HASH')


# Main function to fetch and store recent blocks
def main():
    latest_block = web3.eth.block_number

    # Fetch the latest timestamps and block numbers from Snowflake
    latest_tx_timestamp, latest_tx_block = fetch_latest_timestamp_and_block_number(secret, 'TRANSACTIONS')

    # Determine the start block based on the latest block number from Snowflake
    start_block_tx = latest_tx_block + 1 if latest_tx_block else latest_block - 1

    batch_size = 1000  # Set the batch size to 10000 rows

    # Fetch and store data in batches
    fetch_and_store_transactions(start_block_tx, latest_block, batch_size)

    print("Data has been written to Snowflake tables.")

if __name__ == "__main__":
    main()