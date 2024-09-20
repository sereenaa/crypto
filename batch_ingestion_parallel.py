
from datetime import datetime, timezone
import pandas as pd
import time
from web3 import HTTPProvider, Web3
from requests.exceptions import ReadTimeout
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from get_secret import get_secret
from evm_trace import TraceFrame, CallType, get_calltree_from_geth_trace
from concurrent.futures import ThreadPoolExecutor, as_completed


# Snowflake connection details
secret = get_secret(user='notnotsez')

# Connect to the blockchain RPC endpoint
rpc_url = 'https://rpc.apex.proofofplay.com'
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))

# Utility function to create a Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=secret['user'],
        password=secret['password'],
        account=secret['account'],
        warehouse=secret['warehouse'],
        database='PROOFOFPLAY',
        schema='RAW',
        login_timeout=30  # Increase the timeout for login
    )

# Fetch the latest timestamp and block number from the Snowflake table
def fetch_latest_timestamp_and_block_number(table_name):
    conn = get_snowflake_connection()
    query = f"SELECT MAX(block_time) AS max_time, MAX(block_number) AS max_block FROM {table_name}"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

# Function to perform delta append using pandas to_sql
def delta_append(table_name, data, unique_key):
    if not data:
        return
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    df = pd.DataFrame(data)

    # Fetch existing unique keys
    existing_keys_query = f"SELECT DISTINCT({unique_key}) FROM {table_name}"
    cursor.execute(existing_keys_query)
    existing_keys = {row[0] for row in cursor.fetchall()}

    # Filter out existing data
    df = df[~df[unique_key].isin(existing_keys)]

    # Append new data using write_pandas
    if not df.empty:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        if success:
            print(f"Inserted {nrows} rows into {table_name}")
        else:
            print(f"Failed to insert data into {table_name}")

    cursor.close()
    conn.close()

# Function to fetch transactions
def fetch_transactions(block_num):
    print(f"Fetching transactions from block number {block_num}...")
    data = []
    block = web3.eth.get_block(block_num, full_transactions=True)
    block_time = block['timestamp']
    block_date = time.strftime('%Y-%m-%d', time.gmtime(block_time))
    
    for tx in block['transactions']:
        receipt = web3.eth.get_transaction_receipt(tx['hash'])
        data.append({
            "BLOCK_TIME": datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "BLOCK_NUMBER": block_num,
            "TX_HASH": tx['hash'].hex(),
            "FROM_ADDRESS": tx['from'],
            "TO_ADDRESS": tx.get('to'),
            "VALUE": tx['value'],
            "GAS_LIMIT": tx['gas'],
            "GAS_PRICE": tx['gasPrice'],
            "GAS_USED": receipt['gasUsed'],
            "MAX_FEE_PER_GAS": tx.get('maxFeePerGas'),
            "MAX_PRIORITY_FEE_PER_GAS": tx.get('maxPriorityFeePerGas'),
            "PRIORITY_FEE_PER_GAS": None,
            "EFFECTIVE_GAS_PRICE": receipt.get('effectiveGasPrice'),
            "GAS_USED_FOR_L1": receipt.get('gasUsedForL1'),
            "L1_BLOCK_NUMBER": receipt.get('l1BlockNumber'),
            "NONCE": tx['nonce'],
            "INDEX": tx['transactionIndex'],
            "SUCCESS": receipt['status'] == 1,
            "BLOCK_HASH": block['hash'].hex(),
            "TYPE": tx['type'],
            "ACCESS_LIST": tx.get('accessList'),
            "BLOCK_DATE": block_date,
            "BLOB_VERSIONED_HASHES": None,
            "MAX_FEE_PER_BLOB_GAS": None
        })
    return data

def fetch_and_store_transactions(start_block, end_block, batch_size, max_workers=10):
    data = []
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_transactions, block_num) for block_num in range(start_block, end_block + 1)]
        for future in as_completed(futures):
            data.extend(future.result())
            if len(data) >= batch_size:
                delta_append('TRANSACTIONS', data, 'TX_HASH')
                end_time = time.time()
                print(f"Time taken for batch of {batch_size} transactions: {end_time - start_time:.2f} seconds")
                start_time = time.time()
                data = []

    if data:
        delta_append('TRANSACTIONS', data, 'TX_HASH')


# Function to fetch logs
def fetch_logs(block_num):
    data = []
    block = web3.eth.get_block(block_num, full_transactions=True)
    block_time = block['timestamp']
    block_date = time.strftime('%Y-%m-%d', time.gmtime(block_time))
    block_hash = block['hash'].hex()
    
    for tx in block['transactions']:
        receipt = web3.eth.get_transaction_receipt(tx['hash'])
        tx_from = tx['from']
        tx_to = tx.get('to')
        tx_index = tx['transactionIndex']
        
        for log in receipt['logs']:
            topics = log['topics']
            data.append({
                "BLOCK_TIME": datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "TIMESTAMP": datetime.fromtimestamp(block_time, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "BLOCK_NUMBER": block_num,
                "BLOCK_HASH": block_hash,
                "CONTRACT_ADDRESS": log['address'],
                "TOPIC0": topics[0].hex() if len(topics) > 0 else None,
                "TOPIC1": topics[1].hex() if len(topics) > 1 else None,
                "TOPIC2": topics[2].hex() if len(topics) > 2 else None,
                "TOPIC3": topics[3].hex() if len(topics) > 3 else None,
                "DATA": "0x" + log['data'].hex(),
                "TX_HASH": log['transactionHash'].hex(),
                "INDEX": log['logIndex'],
                "TX_INDEX": tx_index,
                "BLOCK_DATE": block_date,
                "TX_FROM": tx_from,
                "TX_TO": tx_to,
                "BLOB_GAS_PRICE": None,
                "BLOB_GAS_USED": None
            })
    return data

def fetch_and_store_logs(start_block, end_block, batch_size, max_workers=10):
    data = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_logs, block_num) for block_num in range(start_block, end_block + 1)]
        for future in as_completed(futures):
            data.extend(future.result())
            if len(data) >= batch_size:
                start_time = time.time()
                delta_append('LOGS', data, 'TX_HASH')
                end_time = time.time()
                print(f"Time taken for batch of {batch_size} logs: {end_time - start_time:.2f} seconds")
                data = []

    if data:
        delta_append('LOGS', data, 'TX_HASH')


# Function to fetch and process trace data
def fetch_trace_data(txn_hash):
    # Fetch the transaction receipt for additional details
    tx_receipt = web3.eth.get_transaction_receipt(txn_hash)

    # Fetch the block containing the transaction
    block = web3.eth.get_block(tx_receipt['blockNumber'])

    # Fetch the struct logs for the transaction
    struct_logs = web3.manager.request_blocking("debug_traceTransaction", [txn_hash])["structLogs"]

    # Convert struct logs to TraceFrame instances
    trace_frames = [TraceFrame.model_validate(item) for item in struct_logs]

    # Define root node kwargs
    root_node_kwargs = {
        "gas_cost": tx_receipt['gasUsed'],
        "gas_limit": web3.eth.get_transaction(txn_hash)['gas'],
        "address": tx_receipt['to'],
        "calldata": web3.eth.get_transaction(txn_hash)['input'],
        "value": web3.eth.get_transaction(txn_hash)['value'],
        "call_type": CallType.CALL,
    }

    # Get the call-tree node
    calltree = get_calltree_from_geth_trace(trace_frames, **root_node_kwargs)

    # Extract required fields for CSV
    trace_data = {
        "BLOCK_TIME": datetime.fromtimestamp(block.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "BLOCK_NUMBER": block.number,
        "VALUE": web3.eth.get_transaction(txn_hash)['value'],
        "GAS": web3.eth.get_transaction(txn_hash)['gas'],
        "GAS_USED": tx_receipt['gasUsed'],
        "BLOCK_HASH": block.hash.hex(),
        "SUCCESS": tx_receipt['status'] == 1,
        "TX_INDEX": tx_receipt['transactionIndex'],
        "SUB_TRACES": len(calltree.calls) if hasattr(calltree, 'calls') else 0,
        "ERROR": tx_receipt['status'] == 0,
        "TX_SUCCESS": tx_receipt['status'] == 1,
        "TX_HASH": txn_hash,
        "FROM": tx_receipt['from'],
        "TO": tx_receipt['to'],
        "TRACE_ADDRESS": "",  # Placeholder, logic to populate if needed
        "TYPE": "call",  # Placeholder, logic to populate if needed
        "ADDRESS": tx_receipt['to'],
        "CODE": "",  # Placeholder, logic to populate if needed
        "CALL_TYPE": root_node_kwargs["call_type"].value,  # Convert CallType enum to string
        "INPUT": web3.eth.get_transaction(txn_hash)['input'].hex(),
        "OUTPUT": "",  # Placeholder, logic to populate if needed
        "REFUND_ADDRESS": "",  # Placeholder, logic to populate if needed
        "BLOCK_DATE": datetime.fromtimestamp(block.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    }

    return trace_data

def fetch_traces(block_num):
    data = []
    block = web3.eth.get_block(block_num, full_transactions=True)
    for tx in block['transactions']:
        trace_data = fetch_trace_data(tx.hash.hex())
        data.append(trace_data)
    return data

def fetch_and_store_traces(start_block, end_block, batch_size, max_workers=10):
    data = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_traces, block_num) for block_num in range(start_block, end_block + 1)]
        for future in as_completed(futures):
            data.extend(future.result())
            if len(data) >= batch_size:
                start_time = time.time()
                delta_append('TRACES', data, 'TX_HASH')
                end_time = time.time()
                print(f"Time taken for batch of {batch_size} traces: {end_time - start_time:.2f} seconds")
                data = []

    if data:
        delta_append('TRACES', data, 'TX_HASH')

# Main function to fetch and store recent blocks
def main():
    latest_block = web3.eth.block_number

    # Fetch the latest timestamps and block numbers from Snowflake
    latest_tx_timestamp, latest_tx_block = fetch_latest_timestamp_and_block_number('TRANSACTIONS')
    latest_log_timestamp, latest_log_block = fetch_latest_timestamp_and_block_number('LOGS')
    latest_trace_timestamp, latest_trace_block = fetch_latest_timestamp_and_block_number('TRACES')

    # Determine the start block based on the latest block number from Snowflake
    start_block_tx = latest_tx_block + 1 if latest_tx_block else latest_block - 1
    start_block_log = latest_log_block + 1 if latest_log_block else latest_block - 1
    start_block_trace = latest_trace_block + 1 if latest_trace_block else latest_block - 1

    batch_size = 1000  # Set the batch size to 10000 rows

    # Fetch and store data in batches
    fetch_and_store_transactions(start_block_tx, latest_block, batch_size)
    fetch_and_store_logs(start_block_log, latest_block, batch_size)
    fetch_and_store_traces(start_block_trace, latest_block, batch_size)

    print("Data has been written to Snowflake tables.")

if __name__ == "__main__":
    main()