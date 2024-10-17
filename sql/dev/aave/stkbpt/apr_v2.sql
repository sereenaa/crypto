select * from events.aave_stk_token_transfer order by block_timestamp desc limit 100;
select * from events.aave_stk_token_transfer where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_timestamp desc;
select chain, block_timestamp, block_hour, block_day, block_height, tx_hash, log_index, stake_token, symbol, `from`, `to`, `value`, count(*)
from events.aave_stk_token_transfer group by 1,2,3,4,5,6,7,8,9,10,11,12 having count(*) > 1;
select 
  cast(block_day as date) as day
  , sum(value) as num_bpts_unstaked
from `events.aave_stk_token_transfer`
-- from {{ source('events', 'aave_stk_token_transfer') }}
where `to` = '0x0000000000000000000000000000000000000000'
  and stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
group by day
order by day;

with deposits as (
  select 
    cast(block_day as date) as day
    , sum(value) as num_bpts_staked
  from `events.aave_stk_token_transfer`
  -- from {{ source('events', 'aave_stk_token_transfer') }}
  where `from` = '0x0000000000000000000000000000000000000000'
    and stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  group by day
  order by day
)
, withdrawals as (
  select 
    cast(block_day as date) as day
    , sum(value) as num_bpts_unstaked
  from `events.aave_stk_token_transfer`
  -- from {{ source('events', 'aave_stk_token_transfer') }}
  where `to` = '0x0000000000000000000000000000000000000000'
    and stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  group by day
  order by day
)
select 
  COALESCE(d.day, w.day) as day,
  COALESCE(d.num_bpts_staked, 0) as num_bpts_staked,
  COALESCE(w.num_bpts_unstaked, 0) as num_bpts_unstaked
from deposits d
full outer join withdrawals w on d.day = w.day
order by day
;

select timestamp_seconds(block_timestamp) as blck_timestamp, *
from indexer_prod.stk_token_transfer where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_timestamp desc;

select timestamp_seconds(block_timestamp) as blck_timestamp, *
from indexer_prod.stk_token_transfer where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_timestamp desc;

select max(timestamp_seconds(block_timestamp)) from indexer_prod.stk_token_transfer;
select max(timestamp_seconds(block_timestamp)) from indexer_prod.stk_token_transfer;

-- Insert rows from the dev table into the main table
-- INSERT INTO indexer_prod.stk_token_transfer
-- SELECT *
-- FROM indexer_prod.stk_token_transfer
-- WHERE block_timestamp > (
--   SELECT max(block_timestamp)
--   FROM indexer_prod.stk_token_transfer
-- );

select *
from `raw_data.common_balancer_pool_liquidity` 
where symbol = '20wstETH-80AAVE'
order by block_hour desc
;



with date_index as (
  select day as date 
  from unnest(generate_date_array('2021-02-07', current_date(), interval 1 day)) as day --make sure to use this date for the cumulative totals
)
, daily_totals as (
  select 
    cast(block_timestamp as date) as date
    , sum(case 
        when `from` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
        else 0
      end) as num_stk_lp_tokens_minted
    , sum(case 
        when `to` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
        else 0 
      end) as num_stk_lp_tokens_burned
  from events.aave_stk_token_transfer
  -- from {{ source('events', 'aave_stk_token_transfer') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
  group by date
  order by date
) 
, cumulative as (
  select 
    di.date
    , dt.num_stk_lp_tokens_minted
    , dt.num_stk_lp_tokens_burned
    , coalesce(sum(dt.num_stk_lp_tokens_minted) over (order by di.date), 0) - coalesce(sum(dt.num_stk_lp_tokens_burned) over (order by di.date), 0) as cumulative_num_stk_lp_tokens_total
  from date_index di 
  left join daily_totals dt on di.date = dt.date
)
, prices as (
  select 
    date
    , max(wstETH_price) as wstETH_price 
    , max(aave_price) as aave_price 
  from (
    select 
      cast(block_timestamp as date) as date
      , price 
      , pair 
      , case when pair = 'ETH/USD' then price else null end as wstETH_price 
      , case when pair = 'AAVE/USD' then price else null end as aave_price 
      , row_number() over (partition by cast(block_timestamp as date), pair order by block_timestamp) as rn
    from prices.chainlink_subgraph_prices
    -- from {{ source('prices', 'chainlink_subgraph_prices') }}
    where pair in ('AAVE/USD', 'ETH/USD')
    order by block_timestamp desc
  ) where rn = 1
  group by date
  order by date
)
, emission_config as (
  select 
    cast(block_timestamp as date) as date
    -- , asset
    -- , asset_symbol
    , emission_per_day as num_aave_tokens_emission_per_day
  from events.aave_stk_token_asset_config_updated
  -- from {{ source('events', 'aave_stk_token_asset_config_updated') }}
  where asset in ('0xa1116930326d21fb917d5a27f1e9943a9595fb47', '0x9eda81c21c273a82be9bbc19b6a6182212068101')
    and emission_per_day <> 0.0
  order by date
)
, emission_joined as (
  select 
    c.date
    , c.num_stk_lp_tokens_minted --the number of stk LP tokens = the number of LP tokens as they are a 1:1 ratio 
    , c.num_stk_lp_tokens_burned
    , c.cumulative_num_stk_lp_tokens_total
    , last_value(ec.num_aave_tokens_emission_per_day ignore nulls) over (order by c.date) as num_aave_tokens_emission_per_day
    , p.aave_price as aave_price
    , last_value(ec.num_aave_tokens_emission_per_day ignore nulls) over (order by c.date) * p.aave_price as usd_emission_per_day
  from cumulative c
  left join emission_config ec on c.date = ec.date 
  left join prices p on c.date = p.date
  where c.date >= '2023-11-28'
  order by c.date desc
)
, balancer_pool_liquidity_latest_per_day as (
  select *
    , row_number() over (partition by cast(block_hour as date) order by block_hour) as rn
    , rank() over (partition by cast(block_hour as date) order by block_hour) as rank
  from raw_data.common_balancer_pool_liquidity
  -- from {{ source('raw_data', 'common_balancer_pool_liquidity') }}
  where symbol = '20wstETH-80AAVE'
    and block_hour = '2024-10-14T00:00:00.000Z'
  order by block_hour desc

    -- delete from raw_data.common_balancer_pool_liquidity where block_hour = '2024-10-14T00:00:00.000Z' and pool_id = '0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588'
    -- and name = '20wstETH-80AAVE';
)
, daily_swap_volume as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_hour) as date) as date
    , sum(swap_volume_per_hour) as usd_swap_volume_per_day
  from (
    select 
      block_hour
      , total_swap_volume as cumulative_swap_volume
      , lag(total_swap_volume) over (order by block_hour) as previous_cumulative_swap_volume
      , (total_swap_volume - LAG(total_swap_volume) OVER (ORDER BY block_hour)) AS swap_volume_per_hour
    from balancer_pool_liquidity_latest_per_day
    where rn = 1
    order by block_hour desc
  )
  group by date
  order by date
)
-- , aave_wsteth_split as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_hour) as date) as date
    , total_liquidity
    , max(case 
        when token_index = 0 then token_balance 
        else null 
        end) as wstETH_balance 
    , max(case 
        when token_index = 0 then token_latest_usd_price 
        else null 
        end) as wstETH_price 
    , max(case 
        when token_index = 1 then token_balance 
        else null 
        end) as aave_balance 
    , max(case 
        when token_index = 1 then token_latest_usd_price
        else null 
        end) as aave_price
  from balancer_pool_liquidity_latest_per_day
  where rank = 1 -- get the latest block hour per day 
  group by date, total_liquidity
  order by date desc
)
, balancer_pool_tvl_and_supply as (
  select 
    aws.date 
    , bpl.total_liquidity as tvl_pool 
    , bpl.total_shares as num_balancer_lp_tokens 
    , aws.wstETH_balance 
    , aws.aave_balance 
    , usd_swap_volume_per_day
    , bpl.swap_fee 
  from aave_wsteth_split aws 
  left join daily_swap_volume dsv on aws.date = dsv.date 
  left join balancer_pool_liquidity_latest_per_day bpl on aws.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', bpl.block_hour) as date)
  where bpl.rn = 1
  order by aws.date desc
)
, aprs as (
  select
    cast(block_hour as date) as date
    , avg(case 
        when json_extract_scalar(entry, '$.title') = 'wstETH APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
        else null
        end) as interest_bearing_token_yield
    , avg(case 
        when json_extract_scalar(entry, '$.title') = 'Swap fees APR' then cast(json_extract_scalar(entry, '$.apr') as float64)
        else null
        end) as swap_fee_yield
  from raw_data.common_balancer_apiv3_results
  -- from {{ source('raw_data', 'common_balancer_apiv3_results') }}
    , unnest(json_extract_array(dynamicData_aprItems)) as entry
  where symbol = '20wstETH-80AAVE'
    and json_extract_scalar(entry, '$.title') in ('wstETH APR', 'Swap fees APR')
  group by cast(block_hour as date)
  order by date desc
)
select 
  a.date-1 as date
  , round(ej.cumulative_num_stk_lp_tokens_total, 2) as num_stk_lp_tokens_total 
  , round(bp.tvl_pool, 2) as tvl_pool
  , round(bp.num_balancer_lp_tokens, 2) as num_balancer_lp_tokens
  , aws.wstETH_balance as wstETH_balance_pool
  , aws.wstETH_balance * (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) / bp.tvl_pool as wstETH_balance_stk
  , aws.wstETH_price 
  , aws.aave_balance 
  , aws.aave_balance * (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) / bp.tvl_pool as aave_balance_stk
  , aws.aave_price 
  , (aws.wstETH_balance * aws.wstETH_price) + (aws.aave_balance * aws.aave_price) as tvl_pool_check
  , (aws.wstETH_balance * (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) / bp.tvl_pool * aws.wstETH_price)
    +
    (aws.aave_balance * (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) / bp.tvl_pool * aws.aave_price) as tvs_check
  , round(bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total, 2) as tvs 
  , ej.usd_emission_per_day
  , round(bp.usd_swap_volume_per_day, 2) as usd_swap_volume_per_day
  , bp.swap_fee 
  , a.swap_fee_yield
  , a.interest_bearing_token_yield
  , round(ej.usd_emission_per_day / (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) * 365, 2) as aave_emissions_yield
  , a.swap_fee_yield + a.interest_bearing_token_yield + (ej.usd_emission_per_day / (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) * 365) as total_yield
from aprs a
left join aave_wsteth_split aws on a.date = aws.date
left join emission_joined ej on a.date = ej.date 
left join balancer_pool_tvl_and_supply bp on a.date = bp.date
order by a.date 




select * from datamart_aave.aave_stkbpt_apr order by date desc;
select * from datamart_aave.aave_stkbpt_apr order by date desc;
