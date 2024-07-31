
import cryo
import numpy as np
import pandas as pd
import time

from utils.util import *

# Helper function to handle retries
def fetch_with_retry(datatype, blocks, rpc, include_columns=[], retries=3, wait=5):
    for attempt in range(retries):
        try:
            return cryo.collect(
                datatype,
                blocks=blocks,
                rpc=rpc,
                output_format="pandas",
                hex=True,
                requests_per_second=25,
                max_retries=10, 
                initial_backoff=1000,
                include_columns=include_columns
            )
        except Exception as e:
            if "Rate Limit Exceeded" in str(e):
                print(f"Rate limit exceeded. Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                raise e
    raise Exception("Max retries reached")



# Function to fetch and push transactions for a range of blocks using cryo
def fetch_and_push_transactions_with_cryo(secret, rpc, start_block, end_block):
    try:
        start_time = time.time()
        
        # try:
        #     tx_data = cryo.collect(
        #         "txs", 
        #         blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
        #         rpc=rpc, 
        #         output_format="pandas", 
        #         hex=True, 
        #         requests_per_second=25,
        #         max_retries=10, 
        #         initial_backoff=1000
        #     )
        # except Exception as e:
        #     print(f"Error fetching transaction data: {e}")
        #     return
        
        try:
            tx_data = fetch_with_retry(
                "txs", 
                blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
                rpc=rpc,
                include_columns=['timestamp', 'block_number']
            )
        except Exception as e:
            print(f"Error fetching transaction data: {e}")
            return
        
        try:
            # Rename columns to match desired output
            tx_data.rename(columns={
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
                if col not in tx_data.columns:
                    tx_data[col] = None

            # Reorder the DataFrame
            df = tx_data[column_order]
        except Exception as e:
            print(f"Error processing data: {e}")
            return

        end_time = time.time()
        print(f"Time taken to fetch batch of {end_block-start_block} blocks from block {format_number_with_commas(start_block)} to block {format_number_with_commas(end_block)}: {end_time - start_time:.2f} seconds")
        
        try:
            delta_append(secret, 'TRANSACTIONS', df)
        except Exception as e:
            print(f"Error appending data to Delta Lake: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")



# Function to fetch and push logs for a range of blocks using cryo
def fetch_and_push_logs_with_cryo(secret, rpc, start_block, end_block):
    try:
        start_time = time.time()
        
        # try:
        #     log_data = cryo.collect(
        #         "logs", 
        #         blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
        #         rpc=rpc, 
        #         output_format="pandas", 
        #         hex=True, 
        #         requests_per_second=25, 
        #         max_retries=10, 
        #         initial_backoff=1000
        #     )
        # except Exception as e:
        #     print(f"Error fetching log data: {e}")
        #     return

        try:
            log_data = fetch_with_retry(
                "logs", 
                blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
                rpc=rpc,
            )
        except Exception as e:
            print(f"Error fetching transaction data: {e}")
            return
        
        try:
            log_data.columns = log_data.columns.str.upper()
        except Exception as e:
            print(f"Error processing log data columns: {e}")
            return
        
        end_time = time.time()
        print(f"Time taken to fetch batch of {end_block-start_block} blocks from block {format_number_with_commas(start_block)} to block {format_number_with_commas(end_block)}: {end_time - start_time:.2f} seconds")
        
        try:
            delta_append(secret, 'LOGS', log_data)
        except Exception as e:
            print(f"Error appending data to Delta Lake: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")



# Function to fetch and push traces for a range of blocks using cryo
def fetch_and_push_traces_with_cryo(secret, rpc, start_block, end_block):
    try:
        start_time = time.time()
        
        try:
            trace_data = cryo.collect(
                "geth_calls", 
                blocks=[f"{start_block}:{end_block}"], # includes the first block but not the last 
                rpc=rpc, 
                output_format="pandas", 
                hex=True, 
                # requests_per_second=20
            )
        except Exception as e:
            print(f"Error fetching trace data: {e}")
            return
        
        try:
            trace_data.rename(columns={
                'block_number': 'BLOCK_NUMBER',
                'transaction_hash': 'TX_HASH',
                'transaction_index': 'TX_INDEX',
                'from_address': 'FROM_ADDRESS',
                'to_address': 'TO_ADDRESS',
                'value_string': 'VALUE',
                'gas_string': 'GAS_PRICE',
                'gas_used_string': 'GAS_USED',
                'error': 'ERROR',
                'trace_address': 'TRACE_ADDRESS',
                'typ': 'TYPE',
                'input': 'INPUT', 
                'output': 'OUTPUT'
            }, inplace=True)

            # Specify the desired column order
            column_order = [
                'BLOCK_NUMBER', 'TX_HASH', 'TX_INDEX', 'FROM_ADDRESS', 'TO_ADDRESS', 
                'VALUE', 'GAS_PRICE', 'GAS_USED', 'ERROR', 'TRACE_ADDRESS', 'TYPE', 
                'INPUT', 'OUTPUT'
            ]

            # Add missing columns with NaN values
            for col in column_order:
                if col not in trace_data.columns:
                    trace_data[col] = None

            # Reorder the DataFrame
            df = trace_data[column_order]
        except Exception as e:
            print(f"Error processing trace data: {e}")
            return

        end_time = time.time()
        print(f"Time taken to fetch batch of {end_block-start_block} blocks from block {format_number_with_commas(start_block)} to block {format_number_with_commas(end_block)}: {end_time - start_time:.2f} seconds")
        
        try:
            delta_append(secret, 'TRACES', df)
        except Exception as e:
            print(f"Error appending data to Delta Lake: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


