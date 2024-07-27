
from concurrent.futures import ThreadPoolExecutor, as_completed
import cryo
from datetime import datetime, timezone
from evm_trace import TraceFrame, CallType, get_calltree_from_geth_trace
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
from web3 import HTTPProvider, Web3

# Format long numbers with comma notation
def format_number_with_commas(number):
    return "{:,}".format(number)

# Utility function to create a Snowflake connection
def get_snowflake_connection(secret):
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
def fetch_latest_timestamp_and_block_number(secret, table_name):
    conn = get_snowflake_connection(secret)
    query = f"SELECT MAX(block_time) AS max_time, MAX(block_number) AS max_block FROM {table_name}"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

# Function to perform delta append using pandas to_sql
def delta_append(secret, table_name, df):
    conn = get_snowflake_connection(secret)
    cursor = conn.cursor()

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
def fetch_transactions(web3, block_num):
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

# Function to fetch logs
def fetch_logs(web3, block_num):
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

# Function to fetch and process trace data
def fetch_trace_data(web3, txn_hash):
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

def fetch_traces(web3, block_num):
    data = []
    block = web3.eth.get_block(block_num, full_transactions=True)
    for tx in block['transactions']:
        trace_data = fetch_trace_data(tx.hash.hex())
        data.append(trace_data)
    return data


# Function to fetch and push transactions for a range of blocks using cryo
def fetch_and_push_transactions_with_cryo(secret, rpc, start_block, end_block):
    start_time = time.time()
    block_data = cryo.collect(
        "blocks", 
        blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
        rpc=rpc, 
        output_format="pandas", 
        hex=True, 
        requests_per_second=25
    )
    tx_data = cryo.collect(
        "txs", 
        blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
        rpc=rpc, 
        output_format="pandas", 
        hex=True, 
        requests_per_second=25
    )

    merged_data = pd.merge(
        block_data[['block_number', 'timestamp']],
        tx_data,  
        on='block_number', 
        how='left'
    )
    
    # Rename columns to match desired output
    merged_data.rename(columns={
        'timestamp': 'BLOCK_TIME',
        'block_number': 'BLOCK_NUMBER',
        'transaction_hash': 'TX_HASH',
        'from_address': 'FROM_ADDRESS',
        'to_address': 'TO_ADDRESS',
        'value_string': 'VALUE',
        'gas_limit': 'GAS_LIMIT',
        'gas_price': 'GAS_PRICE',
        'gas_used': 'GAS_USED',
        'max_fee_per_gas': 'MAX_FEE_PER_GAS',
        'max_priority_fee_per_gas': 'MAX_PRIORITY_FEE_PER_GAS',
        'priority_fee_per_gas': 'PRIORITY_FEE_PER_GAS',
        'effective_gas_price': 'EFFECTIVE_GAS_PRICE',
        'gas_used_for_l1': 'GAS_USED_FOR_L1',
        'l1_block_number': 'L1_BLOCK_NUMBER',
        'nonce': 'NONCE',
        'transaction_index': 'INDEX',
        'success': 'SUCCESS',
        'block_hash': 'BLOCK_HASH',
        'transaction_type': 'TYPE',
        'access_list': 'ACCESS_LIST',
        'block_date': 'BLOCK_DATE',
        'blob_versioned_hashes': 'BLOB_VERSIONED_HASHES',
        'max_fee_per_blob_gas': 'MAX_FEE_PER_BLOB_GAS'
    }, inplace=True)

    # Specify the desired column order
    column_order = [
        'BLOCK_TIME', 'BLOCK_NUMBER', 'TX_HASH', 'FROM_ADDRESS', 'TO_ADDRESS', 
        'VALUE', 'GAS_LIMIT', 'GAS_PRICE', 'GAS_USED', 'MAX_FEE_PER_GAS', 
        'MAX_PRIORITY_FEE_PER_GAS', 'PRIORITY_FEE_PER_GAS', 'EFFECTIVE_GAS_PRICE', 
        'GAS_USED_FOR_L1', 'L1_BLOCK_NUMBER', 'NONCE', 'INDEX', 'SUCCESS', 
        'BLOCK_HASH', 'TYPE', 'ACCESS_LIST', 'BLOCK_DATE', 'BLOB_VERSIONED_HASHES', 
        'MAX_FEE_PER_BLOB_GAS'
    ]

    # Add missing columns with NaN values
    for col in column_order:
        if col not in merged_data.columns:
            merged_data[col] = None

    # Reorder the DataFrame
    df = merged_data[column_order]
    end_time = time.time()
    print(f"Time taken for batch of {end_block-start_block} blocks: {end_time - start_time:.2f} seconds")
    delta_append(secret, 'TRANSACTIONS', df)