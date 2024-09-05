
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, TimeoutError
import io
import json
from os import cpu_count
import pandas as pd
import requests 
from requests.exceptions import RequestException
import time

from utils.util import *
from config.logger_config import logger  # Import the logger


# Function to get block trace
def get_block_trace(rpc_url, block_number):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_number), {}],
        "id": 1
    }
    response = requests.post(rpc_url, json=payload)
    logger.info(response.json()['result'][0]['result']['structLogs'])
    return response.json()


# Function to get block trace, transform and store in S3
# Reducing the retries will improve overall performance as it won't hold up the other threads
def fetch_transform_store_block_trace(rpc_url, rpc_number, block_number, s3, bucket_name, prefix, retries=2):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_number), {}],
        "id": 1
    }

    try:
        for attempt in range(retries):
            try:
                response = requests.post(rpc_url, json=payload, timeout=60)
                response.raise_for_status()
                trace = response.json()
                
                # Transform and store data
                if 'result' in trace:
                    data = []
                    for tx in trace['result']:
                        tx_hash = tx['txHash']
                        for structLog in tx['result']['structLogs']:
                            storage = structLog.get('storage', {})
                            state_growth_count = sum(1 for val1, val2 in storage.items() if is_null(val1) and not is_null(val2))
                            data.append({
                                'BLOCK_NUMBER': block_number,
                                'TRANSACTION_HASH': tx_hash,
                                'OPCODE': structLog['op'],
                                'GAS': structLog['gas'],
                                'GAS_COST': structLog['gasCost'],
                                'STATE_GROWTH_COUNT': state_growth_count
                            })
                    
                    # Transform data
                    df = pd.DataFrame(data)
                    
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
                    
                    # Store transformed data in S3
                    s3_key = f"{prefix}{block_number}.parquet"
                    buffer = io.BytesIO()
                    merged_df.to_parquet(buffer, index=False)
                    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
                
                return True, block_number
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{retries} failed for block {block_number}: {e} - with rpc {rpc_number}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
    except Exception as e:
        logger.info(f"Block number {block_number} failed using rpc {rpc_number}")
        return False, block_number


# Function to check if a string represents a null value (all zeros)
def is_null(value):
    return value == '0000000000000000000000000000000000000000000000000000000000000000'


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
# def fetch_and_push_raw_opcodes_for_block_range(secret, table_name, rpc_url, rpc_number, blocks_list):
#     num_cpus = cpu_count()
#     logger.info(f"Number of CPUs available: {num_cpus}")

#     # Fetch block traces using ThreadPoolExecutor
#     start_time = time.time()
#     traces = []
#     with ThreadPoolExecutor(max_workers=16) as executor:
#         future_to_block = {executor.submit(get_block_trace_and_block_number, rpc_url, rpc_number, block): block for block in blocks_list}
#         for future in as_completed(future_to_block):
#             try:
#                 trace, block_number = future.result(timeout=130)
#                 logger.info(f'Block number {block_number} has been processed by rpc {rpc_number}')
#                 # traces.append((trace, block_number))
#                 if trace is not None:  # Check if trace is not None before appending
#                     traces.append((trace, block_number))
#             except TimeoutError:
#                 logger.error(f"TimeoutError for block {future_to_block[future]} with rpc {rpc_number}")
#             except Exception as e:
#                 logger.error(f"Exception occurred for block {future_to_block[future]} with rpc {rpc_number}: {e}")
#     end_time = time.time()
#     logger.info(f"Time taken to fetch traces for {str(len(blocks_list))} blocks with rpc {rpc_number}: {end_time - start_time:.2f} seconds")


#     # Process block traces and concatenate into a single DataFrame using ThreadPoolExecutor (this seems to be faster than ProcessPoolExecutor)
#     start_time = time.time()
#     with ThreadPoolExecutor(max_workers=16) as executor:
#         future_to_trace = {executor.submit(process_block_trace, trace): trace for trace in traces}
#         dataframes = [future.result() for future in as_completed(future_to_trace)]
#     df = pd.concat(dataframes, ignore_index=True)
#     end_time = time.time()
#     logger.info(f"Time taken to process block traces for {str(len(blocks_list))} blocks for rpc {rpc_number}: {end_time - start_time:.2f} seconds")


#     # First aggregation level: Aggregate by TRANSACTION_HASH to get MAX_GAS and MIN_GAS
#     start_time = time.time()
#     transaction_agg = df.groupby(['BLOCK_NUMBER', 'TRANSACTION_HASH']).agg(
#         MAX_GAS=('GAS', 'max'),
#         MIN_GAS=('GAS', 'min')
#     ).reset_index()

#     # Second aggregation level: Aggregate by TRANSACTION_HASH and OPCODE
#     opcode_agg = df.groupby(['BLOCK_NUMBER', 'TRANSACTION_HASH', 'OPCODE']).agg(
#         OPCODE_COUNT=('OPCODE', 'size'),
#         SUM_GAS_COST=('GAS_COST', 'sum'),
#         STATE_GROWTH_COUNT=('STATE_GROWTH_COUNT', 'sum')
#     ).reset_index()

#     # Merge the two aggregation results
#     merged_df = pd.merge(transaction_agg, opcode_agg, on=['BLOCK_NUMBER', 'TRANSACTION_HASH'])

#     end_time = time.time()
#     logger.info(f"Time taken to perform transformations on {str(len(blocks_list))} blocks for rpc {rpc_number}: {end_time - start_time:.2f} seconds")


#     # Uncomment the following lines to append the data to Snowflake
#     try:
#         start_time = time.time()
#         delta_append(secret, 'STAGING', table_name, merged_df)
#         end_time = time.time()
#         logger.info(f"Time taken to append {str(len(blocks_list))} blocks to Snowflake for rpc {rpc_number}: {end_time - start_time:.2f} seconds")
#     except Exception as e:
#         logger.info(f"Error appending data to Delta Lake for rpc {rpc_number}: {e}")


# Fetch raw opcodes from RPC and store in S3
def multi_thread_fetch_transform_store_block_trace(s3, bucket_name, prefix, rpc_url, rpc_number, blocks_list):
    num_cpus = cpu_count()
    logger.info(f"Number of CPUs available: {num_cpus}")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_block = {executor.submit(fetch_transform_store_block_trace, rpc_url, rpc_number, block, s3, bucket_name, prefix): block for block in blocks_list}
        for future in as_completed(future_to_block):
            try:
                success, block_number = future.result(timeout=130)
                if success:
                    logger.info(f'Block number {block_number} has been processed and stored by rpc {rpc_number}')
                else:
                    logger.error(f"Failed to fetch and store block {block_number} with rpc {rpc_number}")
            except TimeoutError:
                logger.error(f"TimeoutError for block {future_to_block[future]} with rpc {rpc_number}")
            except Exception as e:
                logger.error(f"Exception occurred for block {future_to_block[future]} with rpc {rpc_number}: {e}")
    end_time = time.time()
    logger.info(f"Time taken to fetch, transform, and store traces for {str(len(blocks_list))} blocks with rpc {rpc_number}: {end_time - start_time:.2f} seconds")
