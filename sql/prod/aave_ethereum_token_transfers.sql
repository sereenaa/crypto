select * from raw_data_aave.aave_ethereum_token_transfers;
select count(*) from raw_data_aave.aave_ethereum_token_transfers;

select 
  block_hash
  , block_number
  , block_timestamp
  , transaction_hash
  , transaction_index
  , event_index
  , batch_index
  , address
  , event_type
  , event_hash
  , event_signature
  , operator_address
  , from_address
  , to_address
  , token_id
  , quantity
  , removed
  , count(*) as count
from raw_data_aave.aave_ethereum_token_transfers
group by 
  block_hash
  , block_number
  , block_timestamp
  , transaction_hash
  , transaction_index
  , event_index
  , batch_index
  , address
  , event_type
  , event_hash
  , event_signature
  , operator_address
  , from_address
  , to_address
  , token_id
  , quantity
  , removed
having count(*) > 1;

select count(*) from raw_data_aave.aave_ethereum_token_transfers_backfill;
select max(block_timestamp) from raw_data_aave.aave_ethereum_token_transfers;
select * from raw_data_aave.aave_ethereum_token_transfers_backfill where block_timestamp = '2024-10-06T00:58:23.000Z'

-- insert into raw_data_aave.aave_ethereum_token_transfers
-- select 
--   block_hash
--   , block_number
--   , block_timestamp
--   , transaction_hash
--   , transaction_index
--   , event_index
--   , batch_index
--   , address
--   , event_type
--   , event_hash
--   , event_signature
--   , operator_address
--   , from_address
--   , to_address
--   , token_id
--   , quantity
--   , removed
--   , _dagster_load_timestamp
--   , null as _dagster_partition_time
--   , null as _dagster_partition_key
--   , null as _dagster_partition_time
-- from raw_data_aave.aave_ethereum_token_transfers_backfill
-- where address = '0x3de27efa2f1aa663ae5d458857e731c129069f29'