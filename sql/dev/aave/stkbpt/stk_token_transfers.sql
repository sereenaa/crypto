select *
from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers` 
where timestamp_trunc(block_timestamp, hour) = timestamp_trunc(timestamp('2024-10-17 07:00:00'), hour)
  and address in ('0x9eda81c21c273a82be9bbc19b6a6182212068101', '0xa1116930326d21fb917d5a27f1e9943a9595fb47', '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d', '0x4da27a545c0c5b758a6ba100e3a049001de870f5')
;

select *
from `bigquery-public-data.goog_blockchain_ethereum_mainnet_us.token_transfers` 
where timestamp_trunc(block_timestamp, hour) between timestamp_trunc(timestamp('2024-10-17 00:00:00'), hour) and timestamp_trunc(timestamp('2024-10-17 07:00:00'), hour)
  and address in ('0x9eda81c21c273a82be9bbc19b6a6182212068101', '0xa1116930326d21fb917d5a27f1e9943a9595fb47', '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d', '0x4da27a545c0c5b758a6ba100e3a049001de870f5')
;



select * from `tokenlogic-data.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_day desc limit 100; --stkAAVEwstETHBPTv2
select count(*) from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'; --stkAAVEwstETHBPTv2
select min(block_timestamp) from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'; --stkAAVEwstETHBPTv2


-- events.aave_stk_token_transfer
-- contains StakeToken Transfer events for all Aave stake tokens
SELECT 
  m.chain
  , timestamp_seconds(m.block_timestamp) as block_timestamp
  , date_trunc(timestamp_seconds(m.block_timestamp), hour) as block_hour
  , date_trunc(timestamp_seconds(m.block_timestamp), day) as block_day
  , m.block_height
  , m.tx_hash
  , m.log_index
  , m.contract_address as stake_token
  , m.symbol
  , m.from_ as `from`
  , m.to as `to`
  , cast(m.value as bignumeric) as value
FROM `tokenlogic-data-dev.indexer_prod.stk_token_transfer` m
where m.contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
order by block_timestamp
  -- and cast(timestamp_seconds(block_timestamp) as date) = '2024-10-07'

select distinct stake_token, symbol from events.aave_stk_token_transfer;
select count(*) from events.aave_stk_token_transfer;
select min(block_timestamp) from events.aave_stk_token_transfer;
select * from raw_data_aave.aave_stk_token_transfers_bigquery limit 5;
select * from raw_data_aave.aave_stk_token_transfers_bigquery_backfill limit 5;
select count(*) from raw_data_aave.aave_stk_token_transfers_bigquery_backfill;
select count(*) from raw_data_aave.aave_stk_token_transfers_bigquery;
select count(*) from tokenlogic-data.raw_data_aave.aave_stk_token_transfers_bigquery;

create or replace view events.aave_stk_token_transfers_bigquery as 
select 
  'ethereum' as chain
  , block_timestamp as block_timestamp
  , date_trunc(block_timestamp, hour) as block_hour
  , date_trunc(block_timestamp, day) as block_day
  , block_number as block_height 
  , transaction_hash as tx_hash 
  , transaction_index as log_index 
  , address as stake_token
  , case 
      when address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' then 'stkAAVEwstETHBPTv2'
      when address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' then 'stkABPT'
      when address = '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d' then 'stkGHO'
      when address = '0x4da27a545c0c5b758a6ba100e3a049001de870f5' then 'stkAAVE'
      end as symbol
  , from_address as `from`
  , to_address as `to` 
  , cast(quantity as bignumeric) as value
from raw_data_aave.aave_stk_token_transfers_bigquery
;

select * from datamart_aave.aave_stk_token_transfers_bigquery where tx_hash = '0xf827724514abd57ca92c5a8964c08567e9bddd40c9e22cd685dd1493cf2c36e1' order by block_timestamp;
select * from events.aave_stk_token_transfer where tx_hash = '0xf827724514abd57ca92c5a8964c08567e9bddd40c9e22cd685dd1493cf2c36e1' order by block_timestamp;


select
  block_hash
  , block_number
  , block_timestamp
  , transaction_hash
  , transaction_index
  , event_index
  , batch_index
  , `address`
  , event_type
  , event_hash
  , event_signature
  , operator_address
  , from_address
  , to_address
  , token_id
  , quantity
  , removed
  , count(*)
from tokenlogic-data.raw_data_aave.aave_stk_token_transfers_bigquery
group by block_hash
  , block_number
  , block_timestamp
  , transaction_hash
  , transaction_index
  , event_index
  , batch_index
  , `address`
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

-- delete from tokenlogic-data.raw_data_aave.aave_stk_token_transfers_bigquery
-- where timestamp_trunc(block_timestamp, hour) = timestamp_trunc(timestamp('2024-10-17 05:00:00'), hour)
-- ;


-- create or replace table tokenlogic-data.raw_data_aave.aave_stk_token_transfers_bigquery as 
-- select * from tokenlogic-data-dev.raw_data_aave.aave_stk_token_transfers_bigquery;

-- insert into tokenlogic-data-dev.raw_data_aave.aave_stk_token_transfers_bigquery
-- select
--   block_hash
--   , block_number
--   , block_timestamp
--   , transaction_hash
--   , transaction_index
--   , event_index
--   , batch_index
--   , `address`
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
--   , null as _dagster_partition_type
--   , null as _dagster_partition_key
--   , null as _dagster_partition_time
-- from tokenlogic-data-dev.raw_data_aave.aave_stk_token_transfers_bigquery_backfill
-- ;

