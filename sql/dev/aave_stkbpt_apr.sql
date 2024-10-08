
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
  from `tokenlogic-data-dev.events.aave_stk_token_transfer`
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
, aave_prices as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) as date
    , price
  from (
    select 
      block_timestamp
      , price
      , row_number() over (partition by cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) order by block_timestamp desc) as row_num
    from `tokenlogic-data-dev.prices.chainlink_subgraph_prices` 
    -- from {{ source('prices', 'chainlink_subgraph_prices') }}
    where pair = 'AAVE/USD'
  ) where row_num = 1
  order by date
)
, emission_config as (
  select 
    cast(block_timestamp as date) as date
    -- , asset
    -- , asset_symbol
    , emission_per_day as num_aave_tokens_emission_per_day
  from `tokenlogic-data-dev.events.aave_stk_token_asset_config_updated`
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
    , ap.price as aave_price
    , last_value(ec.num_aave_tokens_emission_per_day ignore nulls) over (order by c.date) * ap.price as usd_emission_per_day
  from cumulative c
  left join emission_config ec on c.date = ec.date 
  left join aave_prices ap on c.date = ap.date
  where c.date >= '2023-11-28'
  order by c.date desc
)
, balancer_pool_liquidity_latest_per_day as (
  select *
    , rank() over (partition by cast(block_hour as date) order by block_hour desc) as rn
    from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` 
    -- from {{ source('raw_data', 'common_balancer_pool_liquidity') }}
    where symbol = '20wstETH-80AAVE'
    order by block_hour desc
)
, balancer_pool_tvl_and_supply as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_hour) as date) as date
    , tvl_pool
    , num_balancer_lp_tokens
    , swap_fee
    , sum(swap_volume_per_hour) as usd_swap_volume_per_day
  from (
    select 
      block_hour
      , total_liquidity as tvl_pool
      , token_symbol
      , token_balance
      , total_shares as num_balancer_lp_tokens
      , swap_fee
      , total_swap_volume as cumulative_swap_volume
      , lag(total_swap_volume) over (partition by token_symbol order by block_hour) as previous_cumulative_swap_volume
      , (total_swap_volume - LAG(total_swap_volume) OVER (PARTITION BY token_symbol ORDER BY block_hour)) AS swap_volume_per_hour
      , row_number() over (partition by block_hour order by token_symbol) as row_num
    from balancer_pool_liquidity_latest_per_day
    where rn = 1
    order by block_hour
  ) where row_num = 1
  group by date, tvl_pool, num_balancer_lp_tokens, swap_fee
  order by date
)
, swap_fee_apr as (
  select
    cast(block_hour as date) as date
    , avg(cast(json_extract_scalar(entry, '$.apr') as float64)) as apr
  from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results`,
  -- from {{ source('raw_data', 'common_balancer_apiv3_results') }},
    unnest(json_extract_array(dynamicData_aprItems)) as entry
  where symbol = '20wstETH-80AAVE'
    and json_extract_scalar(entry, '$.title') = 'Swap fees APR'
  group by 
    cast(block_hour as date)
    , json_extract_scalar(entry, '$.title')
  order by date
)
select 
  ej.date
  , round(ej.cumulative_num_stk_lp_tokens_total, 2) as cumulative_num_stk_lp_tokens_total --the total number of stk LP tokens = the number of LP tokens that were staked. into the balancer pool as they are a 1:1 ratio 
  , round(ej.usd_emission_per_day, 2) as usd_emission_per_day
  , round(bp.tvl_pool, 2) as tvl_pool
  , round(bp.num_balancer_lp_tokens, 2) as num_balancer_lp_tokens
  , round(bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total, 2) as tvs
  , round(bp.usd_swap_volume_per_day, 2) as usd_swap_volume_per_day
  , bp.swap_fee 
  , case 
      when ej.date >= cast('2024-09-18' as date) then round(bp.usd_swap_volume_per_day * bp.swap_fee, 2)
      else round(ash.usd_daily_swap_fee, 2)
      end as usd_daily_swap_fee
  , round(ej.usd_emission_per_day / (bp.tvl_pool / bp.num_balancer_lp_tokens * ej.cumulative_num_stk_lp_tokens_total) * 365 * 100, 2) as apr_from_emissions
  , case 
      when ej.date >= cast('2024-09-18' as date) then round(cast(sfa.apr as float64)*100, 2) 
      else round(ash.usd_daily_swap_fee / bp.tvl_pool * 365 * 100, 2)
      end as apr_from_swap_fees
from emission_joined ej 
left join balancer_pool_tvl_and_supply bp on ej.date = bp.date
left join swap_fee_apr sfa on ej.date = sfa.date
left join `tokenlogic-data-dev.external_tables.balancer_swap_fees_historical` ash on ej.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', ash.block_hour) as date)+1
-- left join external_tables.balancer_swap_fees_historical ash on ej.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', ash.block_hour) as date)+1
where ej.date >= '2024-02-10'
order by ej.date desc