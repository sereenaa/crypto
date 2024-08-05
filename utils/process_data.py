

import json
import pandas as pd
import requests 

from utils.util import *



# Function to get block trace
def get_block_trace(rpc_url, block_number):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [hex(block_number), {}],
        "id": 1
    }
    response = requests.post(rpc_url, json=payload)
    return response.json()



# Function to check if a string represents a null value (all zeros)
def is_null(value):
    return value == '0000000000000000000000000000000000000000000000000000000000000000'




def fetch_and_push_raw_opcodes(secret, rpc_url, block_number):
    trace = get_block_trace(rpc_url, block_number)

    # Initialize an empty DataFrame
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

        try:
            delta_append(secret, 'OPCODES', merged_df)
        except Exception as e:
            print(f"Error appending data to Delta Lake: {e}")

    else:
        print("Error in fetching trace data")