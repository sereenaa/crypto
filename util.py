
import boto3
from botocore.exceptions import ClientError
import cryo
from datetime import datetime, timezone
from evm_trace import TraceFrame, CallType, get_calltree_from_geth_trace
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time

from config.logs_decode_mapping import *

# Get secret from AWS Secret Manager
def get_secret(user='notnotsez'):

    secret_name = f'snowflake-user-{user}'

    region_name = "ap-southeast-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    return secret



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
def fetch_latest_block_number(secret, table_name):
    conn = get_snowflake_connection(secret)
    query = f"SELECT MAX(block_number) AS max_block FROM {table_name}"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0]



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



# Function to decode logs based on the event type
def decode_log(log):
    topic_0 = log['TOPIC0']
    if topic_0 not in event_hashes:
        return {
            "BLOCK_NUMBER": log['BLOCK_NUMBER'],
            "TRANSACTION_HASH": log['TRANSACTION_HASH'],
            "LOG_INDEX": log['LOG_INDEX'],
            "EVENT": "Unknown", 
            "DATA": log
        }
    
    event_type = event_hashes[topic_0]
    log_data = log['DATA'][2:]  # Remove '0x' prefix

    if event_type == "TransferSingle":
        return decode_transfer_single(log, log_data)
    elif event_type == "ComponentValueSet":
        return decode_component_value_set(log, log_data)
    elif event_type == "TraitValueSet":
        return decode_trait_value_set(log, log_data)
    elif event_type == "Transfer":
        return decode_transfer(log, log_data)
    elif event_type == "PirateTransformCompleted":
        return decode_pirate_transform_completed(log, log_data)
    elif event_type == "RandomNumberDelivered": 
        return decode_random_number_delivered(log, log_data)
    elif event_type == "BattlePending":
        return decode_battle_pending(log, log_data)
    else:
        return {"EVENT": "Unknown", "DATA": log}



def decode_transfer_single(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    operator = "0x" + log['TOPIC1'][26:]
    from_address = "0x" + log['TOPIC2'][26:]
    to_address = "0x" + log['TOPIC3'][26:]
    id = int(log_data[:64], 16)
    value = int(log_data[64:128], 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "TransferSingle",
        "OPERATOR": operator,
        "FROM": from_address,
        "TO": to_address,
        "ID": str(id),
        "VALUE": value
    }



def decode_component_value_set(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    component_id = int(log['TOPIC1'], 16)
    entity = int(log['TOPIC2'], 16)
    data_length = int(log_data[:64], 16) * 2
    data = log_data[64:64 + data_length]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "ComponentValueSet",
        "COMPONENT_ID": str(component_id),
        "ENTITY": str(entity),
        "DATA": "0x" + data
    }



def decode_trait_value_set(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    token_contract = "0x" + log['TOPIC1'][26:]
    token_id = int(log['TOPIC2'], 16)
    trait_id = int(log['TOPIC3'], 16)
    data_length = int(log_data[:64], 16) * 2
    data = log_data[64:64 + data_length]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "TraitValueSet",
        "TOKEN_CONTRACT": token_contract,
        "TOKEN_ID": token_id,
        "TRAIT_ID": trait_id,
        "DATA": "0x" + data
    }



def decode_transfer(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    from_address = "0x" + log['TOPIC1'][26:]
    to_address = "0x" + log['TOPIC2'][26:]
    value = int(log_data[:64], 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "Transfer",
        "FROM": from_address,
        "TO": to_address,
        "VALUE": value
    }



def decode_pirate_transform_completed(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    account = "0x" + log_data[24:64]
    transform_entity = int(log_data[64:128], 16)
    started_count = int(log_data[128:132], 16)
    success_count = int(log_data[132:136], 16)
    nft_token_contract = "0x" + log_data[136:176]
    nft_token_id = int(log_data[176:240], 16)
    
    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "PirateTransformCompleted",
        "ACCOUNT": account,
        "TRANSFORM_ENTITY": transform_entity,
        "STARTED_COUNT": started_count,
        "SUCCESS_COUNT": success_count,
        "NFT_TOKEN_CONTRACT": nft_token_contract,
        "NFT_TOKEN_ID": nft_token_id
    }



def decode_random_number_delivered(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    request_id = int(log['TOPIC1'], 16)
    number = int(log_data, 16)
    
    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "RandomNumberDelivered",
        "REQUEST_ID": request_id,
        "NUMBER": str(number),
    }



def decode_battle_pending(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    battle_entity = int(log['TOPIC1'], 16)
    attacker_entity = int(log['TOPIC2'], 16)
    defender_entity = int(log['TOPIC3'], 16)

    # Decode the data part
    data_bytes = bytes.fromhex(log_data)
    attacker_overloads_offset = int.from_bytes(data_bytes[0:32], byteorder='big')
    defender_overloads_offset = int.from_bytes(data_bytes[32:64], byteorder='big')
    attacker_overloads_count = int.from_bytes(data_bytes[attacker_overloads_offset:attacker_overloads_offset+32], byteorder='big')
    defender_overloads_count = int.from_bytes(data_bytes[defender_overloads_offset:defender_overloads_offset+32], byteorder='big')
    
    attacker_overloads = []
    defender_overloads = []
    
    for i in range(attacker_overloads_count):
        start = attacker_overloads_offset + 32 + i*32
        overload = int.from_bytes(data_bytes[start:start+32], byteorder='big')
        attacker_overloads.append(overload)
    
    for i in range(defender_overloads_count):
        start = defender_overloads_offset + 32 + i*32
        overload = int.from_bytes(data_bytes[start:start+32], byteorder='big')
        defender_overloads.append(overload)
    
    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "BattlePending",
        "BATTLE_ENTITY": str(battle_entity),
        "ATTACKER_ENTITY": str(attacker_entity),
        "DEFENDER_ENTITY": str(defender_entity),
        "ATTACKER_OVERLOADS": str(attacker_overloads),
        "DEFENDER_OVERLOADS": str(defender_overloads)
    }



def transform_logs(secret):
    try:
        table_name = 'LOGS_DECODED'
        try:
            latest_logs_decoded_block = fetch_latest_block_number(secret, table_name)
        except Exception as e:
            print(f"Error fetching latest block number from Snowflake: {e}")
            return
        
        try:
            conn = get_snowflake_connection(secret)
            query = f"""
            SELECT 
                block_number,
                transaction_hash,
                log_index,
                topic0,
                topic1,
                topic2,
                topic3,
                data
            FROM proofofplay.raw.logs 
            WHERE block_number > {latest_logs_decoded_block}
            """
            cursor = conn.cursor()
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
        except Exception as e:
            print(f"Error executing query or fetching data: {e}")
            return
        finally:
            cursor.close()
            conn.close()

        try:
            decoded_logs = [decode_log(log) for log in df.to_dict(orient='records')]
            decoded_logs_df = pd.DataFrame(data=decoded_logs)
            decoded_logs_df = decoded_logs_df.where(pd.notnull(decoded_logs_df), None)

            # Define the desired column order
            desired_columns = [
                "BLOCK_NUMBER", 
                "TRANSACTION_HASH", 
                "LOG_INDEX", 
                "EVENT", 
                "OPERATOR", 
                "FROM", 
                "TO", 
                "ID", 
                "VALUE", 
                "COMPONENT_ID", 
                "ENTITY", 
                "DATA", 
                "TOKEN_CONTRACT", 
                "TOKEN_ID", 
                "TRAIT_ID", 
                "ACCOUNT", 
                "TRANSFORM_ENTITY", 
                "STARTED_COUNT", 
                "SUCCESS_COUNT", 
                "NFT_TOKEN_CONTRACT", 
                "NFT_TOKEN_ID", 
                "REQUEST_ID", 
                "NUMBER", 
                "BATTLE_ENTITY", 
                "ATTACKER_ENTITY", 
                "DEFENDER_ENTITY", 
                "ATTACKER_OVERLOADS", 
                "DEFENDER_OVERLOADS"
            ]
            
            # Reorder the DataFrame columns
            decoded_logs_df = decoded_logs_df.reindex(columns=desired_columns, fill_value=None)
        except Exception as e:
            print(f"Error processing or transforming logs: {e}")
            return

        try:
            delta_append(secret, table_name, decoded_logs_df)
        except Exception as e:
            print(f"Error appending data to Delta Lake: {e}")
    except Exception as e:
        print(f"Unexpected error in transform_logs function: {e}")


