
import csv
import time
from web3 import HTTPProvider, Web3
from requests.exceptions import ReadTimeout

# Connect to the blockchain RPC endpoint
rpc_url = 'https://eth.llamarpc.com'
# rpc_url = 'https://rpc.apex.proofofplay.com'
# web3 = Web3(Web3.HTTPProvider(rpc_url))
# web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 60}))
web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))



# Function to fetch and store raw transactions
def fetch_and_store_transactions(start_block, end_block, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "block_time", "block_number", "tx_hash", "from_address", "to_address", "value", "gas_limit", "gas_price", 
            "gas_used", "max_fee_per_gas", "max_priority_fee_per_gas", "priority_fee_per_gas", "nonce", "index", 
            "success", "block_hash", "type", "access_list", "block_date", "blob_versioned_hashes", "max_fee_per_blob_gas"
        ])
        
        for block_num in range(start_block, end_block + 1):
            block = web3.eth.get_block(block_num, full_transactions=True)
            block_time = block['timestamp']
            block_date = time.strftime('%Y-%m-%d', time.gmtime(block_time))
            
            for tx in block['transactions']:
                receipt = web3.eth.get_transaction_receipt(tx['hash'])
                writer.writerow([
                    block_time,
                    block_num,
                    tx['hash'].hex(),
                    tx['from'],
                    tx.get('to'),
                    tx['value'],
                    tx['gas'],
                    tx['gasPrice'],
                    receipt['gasUsed'],
                    tx.get('maxFeePerGas'),
                    tx.get('maxPriorityFeePerGas'),
                    None,  # priority_fee_per_gas is usually calculated separately
                    tx['nonce'],
                    tx['transactionIndex'],
                    receipt['status'] == 1,
                    block['hash'].hex(),
                    tx['type'],
                    tx.get('accessList'),
                    block_date,
                    None,  # blob_versioned_hashes is not standard
                    None  # max_fee_per_blob_gas is not standard
                ])

# Function to fetch and store raw logs
def fetch_and_store_logs(start_block, end_block, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "block_time", "timestamp", "block_number", "block_hash", "contract_address", "topic0", "topic1", 
            "topic2", "topic3", "data", "tx_hash", "index", "tx_index", "block_date", "tx_from", "tx_to", 
            "blob_gas_price", "blob_gas_used"
        ])
        
        for block_num in range(start_block, end_block + 1):
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
                    writer.writerow([
                        block_time,
                        block_time,  # timestamp is the same as block_time
                        block_num,
                        block_hash,
                        log['address'],
                        topics[0].hex() if len(topics) > 0 else None,
                        topics[1].hex() if len(topics) > 1 else None,
                        topics[2].hex() if len(topics) > 2 else None,
                        topics[3].hex() if len(topics) > 3 else None,
                        "0x" + log['data'].hex(),
                        log['transactionHash'].hex(),
                        log['logIndex'],
                        tx_index,
                        block_date,
                        tx_from,
                        tx_to,
                        None,  # blob_gas_price is not standard
                        None  # blob_gas_used is not standard
                    ])


# Function to fetch and store raw traces with retry mechanism
def fetch_and_store_traces(start_block, end_block, csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["block_number", "tx_hash", "from_address", "to_address", "value", "gas", "input", "output"])
        
        for block_num in range(start_block, end_block + 1):
            for attempt in range(3):  # Retry up to 3 times
                try:
                    traces = web3.manager.request_blocking('debug_traceBlockByNumber', [web3.to_hex(block_num), {}])
                    for trace in traces['result']:
                        writer.writerow([
                            block_num,
                            trace['transactionHash'].hex(),
                            trace['action']['from'],
                            trace['action'].get('to'),
                            trace['action']['value'],
                            trace['action']['gas'],
                            trace['action']['input'],
                            trace.get('result', {}).get('output')
                        ])
                    break  # Break the retry loop if successful
                except ReadTimeout:
                    print(f"Timeout occurred for block {block_num}, attempt {attempt + 1}")
                    time.sleep(5)  # Wait for 5 seconds before retrying

# Fetch and store recent blocks
latest_block = web3.eth.block_number
start_block = latest_block - 1

fetch_and_store_transactions(start_block, latest_block, 'transactions.csv')
fetch_and_store_logs(start_block, latest_block, 'logs.csv')
fetch_and_store_traces(start_block, latest_block, 'traces.csv')

print("Data has been written to transactions.csv, logs.csv, and traces.csv")


# Fetching traces of a block
import csv
from datetime import datetime, timezone
from web3 import HTTPProvider, Web3
from evm_trace import TraceFrame, CallType, get_calltree_from_geth_trace

# Connect to the Ethereum RPC endpoint
rpc_url = "https://rpc.apex.proofofplay.com/"  # Replace with your Ethereum RPC URL
web3 = Web3(HTTPProvider(rpc_url))

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

    # Define root node kwargs (dummy values for example purposes)
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
        "block_time": datetime.fromtimestamp(block.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "block_number": block.number,
        "value": web3.eth.get_transaction(txn_hash)['value'],
        "gas": web3.eth.get_transaction(txn_hash)['gas'],
        "gas_used": tx_receipt['gasUsed'],
        "block_hash": block.hash.hex(),
        "success": tx_receipt['status'] == 1,
        "tx_index": tx_receipt['transactionIndex'],
        "sub_traces": len(calltree.calls) if hasattr(calltree, 'calls') else 0,
        "error": tx_receipt['status'] == 0,
        "tx_success": tx_receipt['status'] == 1,
        "tx_hash": txn_hash,
        "from": tx_receipt['from'],
        "to": tx_receipt['to'],
        "trace_address": "",  # Placeholder, logic to populate if needed
        "type": "call",  # Placeholder, logic to populate if needed
        "address": tx_receipt['to'],
        "code": "",  # Placeholder, logic to populate if needed
        "call_type": CallType.CALL,
        "input": web3.eth.get_transaction(txn_hash)['input'],
        "output": "",  # Placeholder, logic to populate if needed
        "refund_address": "",  # Placeholder, logic to populate if needed
        "block_date": datetime.fromtimestamp(block.timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    }

    return trace_data

# Function to write trace data to CSV
def write_to_csv(data, filename="trace_data.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# Example usage
latest_block_number = web3.eth.block_number
block = web3.eth.get_block(latest_block_number, full_transactions=True)
txn_hash = block.transactions[0].hash.hex()

trace_data = fetch_trace_data(txn_hash)
write_to_csv([trace_data])

print("Trace data written to CSV file successfully.")
