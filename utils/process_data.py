

import json
import logging
from os import cpu_count
import pandas as pd
import requests 
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException


from utils.util import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Function to get block trace
def get_block_trace(rpc_url, block_number):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_number), {}],
        "id": 1
    }
    response = requests.post(rpc_url, json=payload)
    print(response.json()['result'][0]['result']['structLogs'])
    return response.json()


# Function to get block trace
def get_block_trace_and_block_number(rpc_url, block_number, retries=3):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_number), {}],
        "id": 1
    }
    # response = requests.post(rpc_url, json=payload)
    # return response.json(), block_number

    for attempt in range(retries):
        try:
            response = requests.post(rpc_url, json=payload)
            response.raise_for_status()
            return response.json(), block_number
        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed for block {block_number}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
    return None, block_number



# Function to check if a string represents a null value (all zeros)
def is_null(value):
    return value == '0000000000000000000000000000000000000000000000000000000000000000'




def fetch_and_push_raw_opcodes(secret, rpc_url, block_number):
    start_time = time.time()
    trace = get_block_trace(rpc_url, block_number)
    end_time = time.time()
    print(f"Time taken to get_block_trace: {end_time - start_time:.2f} seconds")


    # Initialize an empty DataFrame
    start_time = time.time()
    columns = ['BLOCK_NUMBER', 'TRANSACTION_HASH', 'OPCODE', 'GAS', 'GAS_COST', 'STATE_GROWTH_COUNT']
    df = pd.DataFrame(columns=columns)

    # Process the trace data for all transactions in the block and append to DataFrame
    if 'result' in trace:
        rows = []
        for tx in trace['result']:
            tx_hash = tx['txHash']
            for structLog in tx['result']['structLogs']:
                opcode = structLog['op']
                gas = structLog['gas']
                gas_cost = structLog['gasCost']
                storage = structLog.get('storage', {})
                # storage_json = json.dumps(storage)  # Convert storage dict to JSON string for easy storage
                state_growth_count = sum(1 for val1, val2 in storage.items() if is_null(val1) and not is_null(val2))
                row = {
                    'BLOCK_NUMBER': block_number,
                    'TRANSACTION_HASH': tx_hash,
                    'OPCODE': opcode,
                    'GAS': int(gas),
                    'GAS_COST': int(gas_cost),
                    'STATE_GROWTH_COUNT': state_growth_count
                }
                rows.append(row)
        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

        # First aggregation level: Aggregate by TRANSACTION_HASH to get MAX_GAS and MIN_GAS
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

        # # Add BLOCK_NUMBER to the final DataFrame
        # merged_df['BLOCK_NUMBER'] = block_number

        # # Reorder columns to have BLOCK_NUMBER on the very left
        # cols = ['BLOCK_NUMBER'] + [col for col in merged_df.columns if col != 'BLOCK_NUMBER']
        # merged_df = merged_df[cols]

        end_time = time.time()
        print(f"Time taken to perform transformations: {end_time - start_time:.2f} seconds")


        # try:
        #     start_time = time.time()
        #     delta_append(secret, 'OPCODES', merged_df)
        #     end_time = time.time()
        #     print(f"Time taken to append to Snowflake: {end_time - start_time:.2f} seconds")
        # except Exception as e:
        #     print(f"Error appending data to Delta Lake: {e}")

    else:
        print("Error in fetching trace data")




# multi threading
# Function to process a single block trace and return a DataFrame
def process_block_trace(trace_data):
    trace, block_number = trace_data

    columns = ['BLOCK_NUMBER', 'TRANSACTION_HASH', 'OPCODE', 'GAS', 'GAS_COST', 'STATE_GROWTH_COUNT']
    df = pd.DataFrame(columns=columns)

    if 'result' in trace:
        rows = []
        for tx in trace['result']:
            tx_hash = tx['txHash']
            for structLog in tx['result']['structLogs']:
                opcode = structLog['op']
                gas = structLog['gas']
                gas_cost = structLog['gasCost']
                storage = structLog.get('storage', {})
                state_growth_count = sum(1 for val1, val2 in storage.items() if is_null(val1) and not is_null(val2))
                row = {
                    'BLOCK_NUMBER': block_number,
                    'TRANSACTION_HASH': tx_hash,
                    'OPCODE': opcode,
                    'GAS': int(gas),
                    'GAS_COST': int(gas_cost),
                    'STATE_GROWTH_COUNT': state_growth_count
                }
                rows.append(row)
        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df


# Fetch and push opcodes transformed df to Snowflake for a range of blocks 
# The range of blocks is inclusive of start_block but not including end_block
def fetch_and_push_raw_opcodes_for_block_range(secret, table_name, rpc_url, start_block, end_block):
    num_cpus = cpu_count()
    print(f"Number of CPUs available: {num_cpus}")

    blocks = list(range(start_block, end_block))


    # Fetch block traces using ThreadPoolExecutor
    start_time = time.time()
    traces = []
    with ThreadPoolExecutor(max_workers=num_cpus*5) as executor:
        future_to_block = {executor.submit(get_block_trace_and_block_number, rpc_url, block): block for block in blocks}
        for future in as_completed(future_to_block):
            try:
                trace, block_number = future.result()
                traces.append((trace, block_number))
            except Exception as e:
                print(f"Error fetching traces for block {block_number}: {e}")
    end_time = time.time()
    print(f"Time taken to fetch traces for {str(end_block-start_block)} blocks: {end_time - start_time:.2f} seconds")


    # # Process block traces and concatenate into a single DataFrame using ProcessPoolExecutor
    # start_time = time.time()
    # with ProcessPoolExecutor(max_workers=num_cpus) as executor:
    #     future_to_trace = {executor.submit(process_block_trace, trace): trace for trace in traces}
    #     dataframes = [future.result() for future in as_completed(future_to_trace)]
    # df = pd.concat(dataframes, ignore_index=True)
    # end_time = time.time()
    # print(f"Time taken to process block traces for {str(end_block-start_block)} blocks: {end_time - start_time:.2f} seconds")


    # Process block traces and concatenate into a single DataFrame using ThreadPoolExecutor (this seems to be faster than ProcessPoolExecutor)
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_cpus*5) as executor:
        future_to_trace = {executor.submit(process_block_trace, trace): trace for trace in traces}
        dataframes = [future.result() for future in as_completed(future_to_trace)]
    df = pd.concat(dataframes, ignore_index=True)
    end_time = time.time()
    print(f"Time taken to process block traces for {str(end_block-start_block)} blocks: {end_time - start_time:.2f} seconds")


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
    print(f"Time taken to perform transformations on {str(end_block-start_block)} blocks: {end_time - start_time:.2f} seconds")


    # Uncomment the following lines to append the data to Snowflake
    try:
        start_time = time.time()
        delta_append(secret, table_name, merged_df)
        end_time = time.time()
        print(f"Time taken to append {str(end_block-start_block)} blocks to Snowflake: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error appending data to Delta Lake: {e}")

# Example usage
# fetch_and_push_raw_opcodes(secret, rpc_url, start_block, start_block + 99)