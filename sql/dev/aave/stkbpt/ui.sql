select * from indexer_prod.stk_token_cooldown limit 100;
select * from events.aave_stk_token_cooldown where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' limit 100;

select * from datamart_aave.aave_stkbpt_composition;

select * from datamart_aave.aave_stkbpt_apr order by date desc limit 10;


with bpt as (
  select
    num_stk_lp_tokens_total as total_staked_stkbpt
    , num_balancer_lp_tokens as bpt_supply
    , num_stk_lp_tokens_total / num_balancer_lp_tokens * 100 as perc_stkbpt_of_bpt_supply
    , interest_bearing_token_yield * 100 as wstETH_apy
    , swap_fee_yield * 100 as swap_apy
    , aave_emissions_yield * 100 as aave_apy
    , total_yield * 100 as total_apr
    , tvs
  from datamart_aave.aave_stkbpt_apr
  order by date desc limit 1
)
, bpt_30_days as (
  select
    num_stk_lp_tokens_total as total_staked_stkbpt
    , num_balancer_lp_tokens as bpt_supply
    , num_stk_lp_tokens_total / num_balancer_lp_tokens * 100 as perc_stkbpt_of_bpt_supply
    , interest_bearing_token_yield * 100 as wstETH_apy
    , swap_fee_yield * 100 as swap_apy
    , aave_emissions_yield * 100 as aave_apy
    , total_yield * 100 as total_apr
  from datamart_aave.aave_stkbpt_apr
  where date = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
, unique_stakers as (
  select count(distinct address) as unique_stkbpt_stakers from datamart_aave.aave_stkbpt_composition 
)
, ins as (
  select
    `to` as address
    , 'in' as action
    , sum(value) as num_stk_lp_tokens
  from datamart_aave.aave_stk_token_transfers_bigquery
  -- from {{ ref('aave_stk_token_transfers_bigquery') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
    and cast(block_timestamp as date) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  group by `to`
)
, outs as (
  select
    `from` as address
    , 'out' as action
    , sum(value)*-1 as num_stk_lp_tokens
  from datamart_aave.aave_stk_token_transfers_bigquery
  -- from {{ ref('aave_stk_token_transfers_bigquery') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
    and cast(block_timestamp as date) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  group by `from`
)
, both as (
  select * from ins where address <> '0x0000000000000000000000000000000000000000' --filter the null address out as they cancel each other 
  union all
  select * from outs where address <> '0x0000000000000000000000000000000000000000'
)
, unique_stakers_30_days as (
  select count(address) as unique_stkbpt_stakers from (
    select
      address
      , sum(num_stk_lp_tokens) as num_stk_lp_tokens
    from both
    group by address
    having num_stk_lp_tokens <> 0
  )
)
, top_10_holders as (
  select 
  (select sum(num_stk_lp_tokens) from (
    select num_stk_lp_tokens
    from datamart_aave.aave_stkbpt_composition
    order by perc desc 
    limit 10
  )) / (select total_num_stk_lp_tokens from datamart_aave.aave_stkbpt_composition limit 1) 
    * 100 as perc_top_10_holders
)
, date_index as (
  select day as date 
  from unnest(generate_date_array('2021-02-07', current_date(), interval 1 day)) as day --make sure to use this date for the cumulative totals
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
    '2023-07-11' as date
    , '0xa1116930326d21fb917d5a27f1e9943a9595fb47' as asset
    , 'stkABPT' as asset_symbol
    , 385 as num_aave_tokens_emission_per_day
  
  union all 

  select 
      cast(block_timestamp as date) as date
      , asset
      , asset_symbol
      , emission_per_day as num_aave_tokens_emission_per_day
    from `tokenlogic-data.events.aave_stk_token_asset_config_updated`
    -- from {{ source('events', 'aave_stk_token_asset_config_updated') }}
    where asset in ('0xa1116930326d21fb917d5a27f1e9943a9595fb47', '0x9eda81c21c273a82be9bbc19b6a6182212068101')
      and emission_per_day <> 0.0
    order by date
)
, daily_usd_emissions as (
  select 
    di.date
    , last_value(ec.num_aave_tokens_emission_per_day ignore nulls) over (order by di.date) * p.aave_price as usd_emission_per_day
  from date_index di 
  left join emission_config ec on di.date = ec.date 
  left join prices p on di.date = p.date
)
, yearly_aave_emissions as (
  select sum(usd_emission_per_day) as yearly_aave_emissions
  from daily_usd_emissions
  where date between DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY) and CURRENT_DATE()
)
select 
  bpt.total_staked_stkbpt
  , b3.total_staked_stkbpt
  , bpt.total_staked_stkbpt - b3.total_staked_stkbpt as total_staked_stkbpt_30_day_change_amount
  , bpt.perc_stkbpt_of_bpt_supply
  , bpt.perc_stkbpt_of_bpt_supply - b3.perc_stkbpt_of_bpt_supply as perc_stkbpt_of_bpt_supply_30_day_change_amount
  , us.unique_stkbpt_stakers
  , us.unique_stkbpt_stakers - us3.unique_stkbpt_stakers as unique_stkbpt_stakers_30_day_change_amount
  , bpt.wstETH_apy
  , bpt.swap_apy
  , bpt.aave_apy
  , bpt.total_apr
  , yae.yearly_aave_emissions
  , yae.yearly_aave_emissions / bpt.tvs * 100 as cost_of_coverage
  , tth.perc_top_10_holders
from bpt
left join bpt_30_days b3 on 1=1
left join unique_stakers us on 1=1
left join unique_stakers_30_days us3 on 1=1
left join yearly_aave_emissions yae on 1=1
left join top_10_holders tth on 1=1
;


select min(date) from datamart_aave.aave_stkbpt_apr;
select * from datamart_aave.aave_stkbpt_apr_historical order by date;



select * from datamart_aave.aave_stkbpt_composition;

select * from datamart_aave.aave_stkbpt_metrics_ui;