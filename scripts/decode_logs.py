

import pandas as pd
from web3 import Web3
from util import * 

# Connect to a Web3 provider
w3 = Web3(Web3.HTTPProvider('https://rpc.apex.proofofplay.com'))
secret = get_secret(user='notnotsez')


def get_logs(block_num):
    conn = get_snowflake_connection(secret)
    query = f"""
    select * from proofofplay.raw.logs 
    where block_number = {block_num}
    ;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()
    results_df = pd.DataFrame(data=results)
    return results_df

def get_log(block_num, tx_hash, log_index):
    conn = get_snowflake_connection(secret)
    query = f"""
    select * from proofofplay.raw.logs 
    where block_number = {block_num}
        and transaction_hash = '{tx_hash}'
        and log_index = {log_index}
    ;
    """
    print(query)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetch_pandas_all()
    cursor.close()
    conn.close()
    results_df = pd.DataFrame(data=results)
    return results_df



block_num = 28383732


# Decode all logs
results_df = get_logs(block_num)
decoded_logs = [decode_log(log) for log in results_df.to_dict(orient='records')]
decoded_logs_df = pd.DataFrame(data=decoded_logs)

unknown_df = decoded_logs_df[decoded_logs_df['EVENT'] == 'Unknown']

tx_hash = unknown_df.iloc[0]['TRANSACTION_HASH']
log_index = unknown_df.iloc[0]['LOG_INDEX']
single_log_df = get_log(block_num, tx_hash, log_index)
log = single_log_df.to_dict(orient='records')[0]
log_data = log['DATA'][2:]

decoded_single_log = [decode_log(log) for log in single_log_df.to_dict(orient='records')]







#####################################LOOP######################################
def decode_logs_for_block(block_num):
    # Decode all logs for a block
    results_df = get_logs(block_num)
    decoded_logs = [decode_log(log) for log in results_df.to_dict(orient='records')]
    decoded_logs_df = pd.DataFrame(data=decoded_logs)
    return decoded_logs_df


# Initialize the starting block number
start_block_num = 28381766
# Define the number of blocks to process
num_blocks = 100  # Change this to the desired number of blocks to process

# Initialize an empty DataFrame to store all unknown events
all_unknown_df = pd.DataFrame(columns=['TRANSACTION_HASH', 'LOG_INDEX'])

# Loop through the blocks
for block_num in range(start_block_num, start_block_num + num_blocks):
    decoded_logs_df = decode_logs_for_block(block_num)
    unknown_df = decoded_logs_df[decoded_logs_df['EVENT'] == 'Unknown']
    
    # Append the TRANSACTION_HASH and LOG_INDEX of unknown events to the cumulative DataFrame
    all_unknown_df = pd.concat([all_unknown_df, unknown_df[['TRANSACTION_HASH', 'LOG_INDEX']]], ignore_index=True)

# Now all_unknown_df contains the TRANSACTION_HASH and LOG_INDEX of all unknown logs from the specified blocks
print(all_unknown_df)
print(all_unknown_df.iloc[0]['TRANSACTION_HASH'])