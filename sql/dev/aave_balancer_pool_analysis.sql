select count(*) from raw_data_aave.aave_ethereum_token_transfers;
select * from raw_data_aave.aave_ethereum_token_transfers limit 5;

select * from raw_data_aave.aave_ethereum_token_transfers 
where to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
limit 5;

select * from raw_data_aave.aave_ethereum_token_transfers
where address = '0x3de27efa2f1aa663ae5d458857e731c129069f29'
order by block_timestamp desc;

select count(*) from raw_data_aave.aave_ethereum_token_transfers_backfill;
-- insert into raw_data_aave.aave_ethereum_token_transfers
-- select block_hash, block_number, block_timestamp, transaction_hash, transaction_index, event_index, batch_index, address, event_type, event_hash, event_signature, operator_address, from_address, to_address, token_id, quantity, removed, _dagster_load_timestamp, null as _dagster_partition_time, null as _dagster_partition_key, null as _dagster_partition_time
-- from raw_data_aave.aave_ethereum_token_transfers_backfill
-- where address = '0x3de27efa2f1aa663ae5d458857e731c129069f29';


select * from raw_data_aave.aave_ethereum_token_transfers where transaction_hash = '0x067e7e62767d517b9e04ac1ef86a9ab47066014e2ab378c545b10bfcb7d35990';


select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged order by block_timestamp;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill order by block_timestamp;

-- insert into raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
-- select 
--   block_hash, block_number, block_timestamp, transaction_hash, transaction_index, 
--   log_index, address, data, topics, removed, token_1, token_2, 
--   delta_1, delta_2, protocolFeeAmount_1, protocolFeeAmount_2, _dagster_load_timestamp, 
--   null as _dagster_partition_type, null as _dagster_partition_key, null as _dagster_partition_time
-- from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;

select
  cast(block_timestamp as date) as day
  , sum(cast(delta_1 as float64)/1e18) as wstETH_amount
  , sum(cast(delta_2 as float64)/1e18) as aave_amount
from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
group by day
order by day;