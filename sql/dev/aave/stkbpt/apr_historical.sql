
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
  from `tokenlogic-data.events.aave_stk_token_transfer`
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
    from `tokenlogic-data.prices.chainlink_subgraph_prices` 
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
  from `tokenlogic-data.events.aave_stk_token_asset_config_updated`
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
    from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` 
    -- from {{ source('raw_data', 'common_balancer_pool_liquidity') }}
    where symbol = '20wstETH-80AAVE'
    order by block_hour desc
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
, balancer_pool_tvl_and_supply as (
  select 
    dsv.date 
    , bpl.total_liquidity as tvl_pool 
    , bpl.total_shares as num_balancer_lp_tokens 
    , usd_swap_volume_per_day
    , bpl.swap_fee 
  from daily_swap_volume dsv
  left join balancer_pool_liquidity_latest_per_day bpl on dsv.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', bpl.block_hour) as date)
  where bpl.rn = 1
  order by dsv.date desc
)
select 
  ej.date-1 as date
  , round(ej.cumulative_num_stk_lp_tokens_total, 2) as num_stk_lp_tokens_total 
  , round(bp.tvl_pool, 2) as tvl_pool
  , round(bp.num_balancer_lp_tokens, 2) as num_balancer_lp_tokens
  , round(bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total, 2) as tvs 
  , ej.usd_emission_per_day
from emission_joined ej 
left join balancer_pool_tvl_and_supply bp on ej.date = bp.date
order by ej.date 


select * from tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr_historical;
select * from tokenlogic-data.datamart_aave.aave_stkbpt_apr_historical;





