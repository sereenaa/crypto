
import pandas as pd

from config.logs_decode_mapping import *
from utils.util import *
from utils.cryo_fetch import *

# Function to decode logs based on the event type
def decode_log(log):
    topic_0 = log['TOPIC0']
    topic_1 = log['TOPIC1']
    topic_2 = log['TOPIC2']
    topic_3 = log['TOPIC3']
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
    elif event_type == "RequestRandomNumber":
        return decode_request_random_number(log, log_data)
    elif event_type == "CountSet":
        return decode_count_set(log, log_data)
    elif event_type == "DungeonLootGranted":
        return decode_dungeon_loot_granted(log, log_data)
    elif event_type == "UpgradePirateLevel":
        return decode_upgrade_pirate_level(log, log_data)
    elif event_type == "ComponentValueRemoved":
        return decode_component_type_removed(log, log_data)
    elif event_type == "PerformGameItemAction": 
        return decode_perform_game_item_action(log, log_data)
    elif event_type == "OperatorRegistered":
        return decode_operator_registered(log, log_data)
    elif event_type == "Approval":
        return decode_approval(log, log_data,  topic_1, topic_2)
    elif event_type == "FundsForwardedWithData":
        return decode_funds_forwarded_with_data(log, log_data)
    elif event_type == "MilestoneClaimed":
        return decode_milestone_claimed(log, log_data, topic_1, topic_2, topic_3)
    elif event_type == "ApprovalForAll":
        return decode_approve_for_all(log, log_data, topic_1, topic_2)
    elif event_type == "TransferBatch":
        return decode_transfer_batch(log, log_data, topic_1,  topic_2, topic_3)
    elif event_type == "TicketCreated":
        return decode_ticket_created(log, log_data, topic_1)
    elif event_type == "RedeemScheduled":
        return decode_redeem_scheduled(log, log_data, topic_1, topic_2, topic_3)
    elif event_type == "L2ToL1Tx":
        return decode_l2_to_l1_tx(log, log_data, topic_1, topic_2, topic_3)
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
        "VALUE": str(value)
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
        "TOKEN_ID": str(token_id),
        "TRAIT_ID": str(trait_id),
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
        "VALUE": str(value)
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
        "TRANSFORM_ENTITY": str(transform_entity),
        "STARTED_COUNT": str(started_count),
        "SUCCESS_COUNT": str(success_count),
        "NFT_TOKEN_CONTRACT": nft_token_contract,
        "NFT_TOKEN_ID": str(nft_token_id)
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
        "REQUEST_ID": str(request_id),
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



def decode_request_random_number(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    request_id = int(log['TOPIC1'], 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "RequestRandomNumber",
        "REQUEST_ID": str(request_id)
    }



def decode_count_set(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    # Decode the data part
    data_bytes = bytes.fromhex(log_data)
    entity = int.from_bytes(data_bytes[0:32], byteorder='big')
    key = int.from_bytes(data_bytes[32:64], byteorder='big')
    new_total = int.from_bytes(data_bytes[64:96], byteorder='big')

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "CountSet",
        "ENTITY": str(entity),
        "KEY": str(key),
        "NEW_TOTAL": str(new_total)
    }



def decode_dungeon_loot_granted(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    account = log['TOPIC1']

    battle_entity = int(log['TOPIC2'], 16)
    
    # Decode the data part
    data_bytes = bytes.fromhex(log_data)
    # account = '0x' + data_bytes[12:32].hex()
    scheduled_start_timestamp = int.from_bytes(data_bytes[0:32], byteorder='big')
    map_entity = int.from_bytes(data_bytes[32:64], byteorder='big')
    node = int.from_bytes(data_bytes[64:96], byteorder='big')

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "DungeonLootGranted",
        "ACCOUNT": account,
        "BATTLE_ENTITY": str(battle_entity),
        "SCHEDULED_START_TIMESTAMP": str(scheduled_start_timestamp),
        "MAP_ENTITY": str(map_entity),
        "NODE": str(node)
    }



def decode_upgrade_pirate_level(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    token_contract = log['TOPIC1']
    token_id = int(log['TOPIC2'], 16)
    
    # Decode the data part
    data_bytes = bytes.fromhex(log_data)
    new_level = int.from_bytes(data_bytes[0:32], byteorder='big')

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "UpgradePirateLevel",
        "TOKEN_CONTRACT": token_contract,
        "TOKEN_ID": str(token_id),
        "NEW_LEVEL": str(new_level)
    }



def decode_component_type_removed(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    component_id = int(log['TOPIC1'], 16)
    entity = int(log['TOPIC2'], 16)
    
    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "ComponentValueRemoved",
        "COMPONENT_ID": str(component_id),
        "ENTITY": str(entity)
    }



def decode_perform_game_item_action(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    # Decode the data part
    data_bytes = bytes.fromhex(log_data)
    account = '0x' + data_bytes[12:32].hex()
    token_contract = '0x' + data_bytes[44:64].hex()
    token_id = int.from_bytes(data_bytes[64:96], byteorder='big')
    amount = int.from_bytes(data_bytes[96:128], byteorder='big')
    action_id = int.from_bytes(data_bytes[128:160], byteorder='big')

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "PerformGameItemAction",
        "ACCOUNT": account,
        "TOKEN_CONTRACT": token_contract,
        "TOKEN_ID": str(token_id),
        "AMOUNT": str(amount),
        "ACTION_ID": str(action_id)
    }



def decode_operator_registered(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']
    
    # Decode the data part
    data_bytes = bytes.fromhex(log_data[2:])  # Remove '0x' prefix and convert to bytes
    player = '0x' + data_bytes[11:31].hex()
    operator = '0x' + data_bytes[43:63].hex()
    expiration = int.from_bytes(data_bytes[64:96], byteorder='big')

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "OperatorRegistered",
        "PLAYER": player,
        "OPERATOR": operator,
        "EXPIRATION": str(expiration)
    }



def decode_approval(log, log_data, topic_1, topic_2):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the data part
    data_bytes = bytes.fromhex(log_data[2:])  # Remove '0x' prefix and convert to bytes
    value = int.from_bytes(data_bytes[-32:], byteorder='big')

    # Decode the topics
    owner = '0x' + topic_1[26:]
    spender = '0x' + topic_2[26:]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "Approval",
        "OWNER": owner,
        "SPENDER": spender,
        "VALUE": str(value)
    }



def decode_funds_forwarded_with_data(log, log_data):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the data part
    data_bytes = bytes.fromhex(log_data[2:])  # Remove '0x' prefix and convert to bytes
    data = data_bytes[63:67].hex()

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "FundsForwardedWithData",
        "DATA": "0x" + data
    }



def decode_milestone_claimed(log, log_data, topic_1, topic_2, topic_3):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the data part
    data_bytes = bytes.fromhex(log_data[2:])  # Remove '0x' prefix and convert to bytes
    milestone_index = int.from_bytes(data_bytes[-2:], byteorder='big')

    # Decode the topics
    owner = '0x' + topic_1[26:]
    token_contract = '0x' + topic_2[26:]
    token_id = int(topic_3, 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "MilestoneClaimed",
        "OWNER": owner,
        "TOKEN_CONTRACT": token_contract,
        "TOKEN_ID": str(token_id),
        "MILESTONE_INDEX": str(milestone_index)
    }


def decode_approve_for_all(log, log_data, topic_1, topic_2):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the data part
    data_bytes = bytes.fromhex(log_data[2:])  # Remove '0x' prefix and convert to bytes
    approved = bool(int.from_bytes(data_bytes[-1:], byteorder='big'))

    # Decode the topics
    account = '0x' + topic_1[26:]
    operator = '0x' + topic_2[26:]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "ApprovalForAll",
        "ACCOUNT": account,
        "OPERATOR": operator,
        "APPROVED": approved
    }



def decode_transfer_batch(log, log_data, topic_1,  topic_2, topic_3):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the topics
    operator = '0x' + topic_1[26:]
    from_address = '0x' + topic_2[26:]
    to_address = '0x' + topic_3[26:]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "TransferBatch",
        "OPERATOR": operator,
        "FROM": from_address,
        "TO": to_address,
    }




def decode_ticket_created(log, log_data, topic_1):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the topics
    ticket_id = '0x' + topic_1[2:]

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "TicketCreated",
        "TICKET_ID": ticket_id
    }



def decode_redeem_scheduled(log, log_data, topic_1, topic_2, topic_3):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the topics
    ticket_id = '0x' + topic_1[2:]
    retry_tx_hash = '0x' + topic_2[2:]
    sequence_num = int(topic_3, 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "RedeemScheduled",
        "TICKET_ID": ticket_id,
        "RETRY_TX_HASH": retry_tx_hash,
        "SEQUENCE_NUM": str(sequence_num),
    }



def decode_l2_to_l1_tx(log, log_data, topic_1, topic_2, topic_3):
    block_number = log['BLOCK_NUMBER']
    transaction_hash = log['TRANSACTION_HASH']
    log_index = log['LOG_INDEX']

    # Decode the topics
    destination = '0x' + topic_1[26:]
    hash_value = int(topic_2, 16)
    position = int(topic_3, 16)

    return {
        "BLOCK_NUMBER": block_number,
        "TRANSACTION_HASH": transaction_hash,
        "LOG_INDEX": log_index,
        "EVENT": "L2ToL1Tx",
        "DESTINATION": destination,
        "HASH": str(hash_value),
        "POSITION": str(position),
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
                "DEFENDER_OVERLOADS", 
                "KEY",
                "NEW_TOTAL",
                "SCHEDULED_START_TIMESTAMP",
                "MAP_ENTITY",
                "NODE",
                "NEW_LEVEL",
                "AMOUNT",
                "ACTION_ID",
                "PLAYER", 
                "EXPIRATION", 
                "OWNER",
                "SPENDER",
                "MILESTONE_INDEX",
                "APPROVED", # bool
                "TICKET_ID",
                "RETRY_TX_HASH",
                "SEQUENCE_NUM",
                "DESTINATION",
                "HASH",
                "POSITION",
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


