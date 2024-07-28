

import pandas as pd
from web3 import Web3
from get_secret import get_secret
from util import * 

# Connect to a Web3 provider
w3 = Web3(Web3.HTTPProvider('https://rpc.apex.proofofplay.com'))
secret = get_secret(user='notnotsez')

conn = get_snowflake_connection(secret)
query = f"""
select * from proofofplay.raw.logs where block_number = 28382322 and transaction_hash = '0x7a4a4a586295c9b87b8a42f3b3b5709c475715d2feebcf9415dd3c8c668cc545';
"""
cursor = conn.cursor()
cursor.execute(query)
df = cursor.fetch_pandas_all()
cursor.close()
conn.close()

# Known event signatures and their types
event_signatures = {
    "TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)": "TransferSingle",
    "ComponentValueSet(uint256 indexed componentId, uint256 indexed entity, bytes data)": "ComponentValueSet",
    "TraitValueSet(address indexed tokenContract, uint256 indexed tokenId, uint256 indexed traitId, bytes value)": "TraitValueSet",
    "Transfer(address indexed from, address indexed to, uint256 value)": "Transfer",
    "PirateTransformCompleted(address account, uint256 transformEntity, uint16 startedCount, uint16 successCount, address nftTokenContract, uint256 nftTokenId)": "PirateTransformCompleted"
}

# Precompute the hashes of the event signatures
event_hashes = {
    '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62': "TransferSingle",
    '0x6837901feacdbb2b5f689b180c02268b287523b334088077ba4c35daf4fe34a8': "ComponentValueSet",
    '0x161f549f01144f89b39ecb5813ffb68eb7d96745f0670fd34d54edfc69c6cd8f': "TraitValueSet",
    '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': "Transfer",
    '0xb5646821f44ee053a85b0a7363ee209ee3f7bb38e035c63a89b0ac31532200b8': "PirateTransformCompleted"
}
# Function to decode logs based on the event type
def decode_log(log):
    topic_0 = log['TOPIC0']
    if topic_0 not in event_hashes:
        return {"event": "Unknown", "data": log}
    
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
        return decode_pirate_transform_completed(log_data)
    else:
        return {"event": "Unknown", "data": log}

def decode_transfer_single(log, log_data):
    operator = "0x" + log['TOPIC1'][26:]
    from_address = "0x" + log['TOPIC2'][26:]
    to_address = "0x" + log['TOPIC3'][26:]
    id = int(log_data[:64], 16)
    value = int(log_data[64:128], 16)

    return {
        "EVENT": "TransferSingle",
        "OPERATOR": operator,
        "FROM": from_address,
        "TO": to_address,
        "ID": id,
        "VALUE": value
    }

def decode_component_value_set(log, log_data):
    component_id = int(log['TOPIC1'], 16)
    entity = int(log['TOPIC2'], 16)
    data_length = int(log_data[:64], 16) * 2
    data = log_data[64:64 + data_length]

    return {
        "EVENT": "ComponentValueSet",
        "COMPONENT_ID": component_id,
        "ENTITY": entity,
        "DATA": "0x" + data
    }

def decode_trait_value_set(log, log_data):
    token_contract = "0x" + log['TOPIC1'][26:]
    token_id = int(log['TOPIC2'], 16)
    trait_id = int(log['TOPIC3'], 16)
    data_length = int(log_data[:64], 16) * 2
    data = log_data[64:64 + data_length]

    return {
        "EVENT": "TraitValueSet",
        "TOKEN_CONTRACT": token_contract,
        "TOKEN_ID": token_id,
        "TRAID_ID": trait_id,
        "DATA": "0x" + data
    }

def decode_transfer(log, log_data):
    from_address = "0x" + log['TOPIC1'][26:]
    to_address = "0x" + log['TOPIC2'][26:]
    value = int(log_data[:64], 16)

    return {
        "EVENT": "Transfer",
        "FROM": from_address,
        "TO": to_address,
        "VALUE": value
    }

def decode_pirate_transform_completed(log_data):
    account = "0x" + log_data[24:64]
    transform_entity = int(log_data[64:128], 16)
    started_count = int(log_data[128:132], 16)
    success_count = int(log_data[132:136], 16)
    nft_token_contract = "0x" + log_data[136:176]
    nft_token_id = int(log_data[176:240], 16)
    
    return {
        "EVENT": "PirateTransformCompleted",
        "ACCOUNT": account,
        "TRANSFORM_ENTITY": transform_entity,
        "STARTED_COUNT": started_count,
        "SUCCESS_COUNT": success_count,
        "NFT_TOKEN_CONTRACT": nft_token_contract,
        "NFT_TOKEN_ID": nft_token_id
    }

# Decode all logs
decoded_logs = [decode_log(log) for log in df.to_dict(orient='records')]

# Print decoded logs
for decoded_log in decoded_logs:
    print(decoded_log)


decoded_logs_df = pd.DataFrame(data=decoded_logs)
decoded_logs_df.to_csv('decoded_logs_0x7a4a4a586295c9b87b8a42f3b3b5709c475715d2feebcf9415dd3c8c668cc545.csv')