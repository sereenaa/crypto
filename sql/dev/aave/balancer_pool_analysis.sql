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


select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged order by block_timestamp desc;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill order by block_timestamp;
select block_hash, block_number, block_timestamp, transaction_hash, transaction_index, log_index, `address`, data, topics, removed, token_1, token_2, delta_1, delta_2, protocolFeeAmount_1, protocolFeeAmount_2, count(*)
from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 having count(*) > 1;

-- insert into raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
-- select 
--   block_hash, block_number, block_timestamp, transaction_hash, transaction_index, 
--   log_index, address, data, topics, removed, token_1, token_2, 
--   delta_1, delta_2, protocolFeeAmount_1, protocolFeeAmount_2, _dagster_load_timestamp, 
--   null as _dagster_partition_type, null as _dagster_partition_key, null as _dagster_partition_time
-- from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;

select distinct token_1 from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged order by block_timestamp desc;


with prices as (
  select
    day 
    , max(case when asset = 'AAVE' then price end) as aave_price --remove the nulls and duplicate rows
    , max(case when asset = 'ETH' then price end) as eth_price --remove the nulls and duplicate rows
  from (
    select 
      cast(block_timestamp as date) as day 
      , asset 
      , price
      , row_number() over (partition by cast(block_timestamp as date), asset order by block_timestamp desc) as row_num
    from prices.chainlink_subgraph_prices
    -- from {{ source('prices', 'chainlink_subgraph_prices') }}
    where pair in ('AAVE/USD', 'ETH/USD')
  ) 
  where row_num = 1
  group by day
  order by day desc
)
, deposits as (
  select 
    cast(block_timestamp as date) as day
    , sum(case when delta_1 > 0 and delta_2 = 0 then cast(delta_1 as float64)/1e18 else null end) as wstETH_only_deposit
    , sum(case when delta_1 < 0 and delta_2 = 0 then cast(delta_1 as float64)/1e18 else null end) as wstETH_only_withdrawal
    , sum(case when delta_1 > 0 then cast(delta_1 as float64)/1e18 else null end) as wstETH_total_deposit 
    , sum(case when delta_1 < 0 then cast(delta_1 as float64)/1e18 else null end) as wstETH_total_withdrawal
    , sum(case when delta_2 > 0 and delta_1 = 0 then cast(delta_2 as float64)/1e18 else null end) as aave_only_deposit
    , sum(case when delta_2 < 0 and delta_1 = 0 then cast(delta_2 as float64)/1e18 else null end) as aave_only_withdrawal
    , sum(case when delta_2 > 0 then cast(delta_2 as float64)/1e18 else null end) as aave_total_deposit 
    , sum(case when delta_2 < 0 then cast(delta_2 as float64)/1e18 else null end) as aave_total_withdrawal
    , sum(cast(delta_1 as float64)/1e18) as wstETH_net
    , sum(cast(delta_2 as float64)/1e18) as aave_net
    , p.aave_price 
    , p.eth_price
  from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged ap
  -- from {{ source('raw_data_aave', 'aave_20wstETH_80AAVE_PoolBalanceChanged') }} ap
  left join prices p
    on cast(ap.block_timestamp as date) = p.day
  group by day, p.aave_price, p.eth_price
  order by day desc
)
select 
  day 
  , eth_price 
  , aave_price 
  , wstETH_only_deposit 
  , wstETH_only_deposit * eth_price as wstETH_only_deposit_value
  , wstETH_only_withdrawal
  , wstETH_only_withdrawal * eth_price as wstETH_only_withdrawal_value
  , wstETH_total_deposit
  , wstETH_total_deposit * eth_price as wstETH_total_deposit_value 
  , wstETH_total_withdrawal 
  , wstETH_total_withdrawal * eth_price as wstETH_total_withdrawal_value 
  , aave_only_deposit
  , aave_only_deposit * aave_price as aave_only_deposit_value 
  , aave_only_withdrawal
  , aave_only_withdrawal * aave_price as aave_only_withdrawal_value 
  , aave_total_deposit 
  , aave_total_deposit * aave_price as aave_total_deposit_value 
  , aave_total_withdrawal 
  , aave_total_withdrawal * aave_price as aave_total_withdrawal_value
  , wstETH_net 
  , wstETH_net * eth_price as wstETH_net_value
  , aave_net
  , aave_net * aave_price as aave_net_value
from deposits
;



select distinct pair from prices.chainlink_subgraph_prices order by pair; --AAVE/USD ETH/USD
select * from prices.chainlink_subgraph_prices where pair in ('AAVE/USD', 'ETH/USD');

select
  cast(block_timestamp as date) as day
  , sum(cast(delta_1 as float64)/1e18) as wstETH_amount
  , sum(cast(delta_2 as float64)/1e18) as aave_amount
from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
group by day
order by day;


select * from tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity limit 5;



with balancer_pool_liquidity_latest_per_day as (
  select *
    , rank() over (partition by cast(block_hour as date) order by block_hour desc) as rn
    from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` 
    -- from {{ source('raw_data', 'common_balancer_pool_liquidity') }}
    where symbol = '20wstETH-80AAVE'
    order by block_hour desc
)
, balancer_pool_tvl as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_hour) as date) as date
    , tvl_pool
  from (
    select 
      block_hour
      , total_liquidity as tvl_pool
      , token_symbol
      , row_number() over (partition by block_hour order by token_symbol) as row_num
    from balancer_pool_liquidity_latest_per_day
    where rn = 1
    order by block_hour
  ) where row_num = 1
  group by date, tvl_pool
  order by date
)
select
  cast(block_hour as date) as date
  , avg(case 
      when json_extract_scalar(entry, '$.title') = 'wstETH APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
      else null
      end) as yield_bearing_tokens_apr
  , avg(case 
      when json_extract_scalar(entry, '$.title') = 'Swap fees APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
      else null
      end) as swap_fees_apr
  , avg(case 
      when json_extract_scalar(entry, '$.title') = 'wstETH APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
      else null
      end) + 
      avg(case 
      when json_extract_scalar(entry, '$.title') = 'Swap fees APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
      else null
      end) as total_apr
  , bpt.tvl_pool 
from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` cbar
left join balancer_pool_tvl bpt on cast(cbar.block_hour as date) = bpt.date
,
  -- from {{ source('raw_data', 'common_balancer_apiv3_results') }},
  unnest(json_extract_array(dynamicData_aprItems)) as entry
where symbol = '20wstETH-80AAVE'
  and json_extract_scalar(entry, '$.title') in ('wstETH APR', 'Swap fees APR')
group by cast(cbar.block_hour as date), bpt.tvl_pool
order by date desc;



select * from datamart_aave.aave_balancer_pool_20wsteth_80aave_tvl_apr;

