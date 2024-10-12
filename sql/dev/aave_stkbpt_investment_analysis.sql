

select * from raw_data.common_balancer_pool_liquidity
where pool_address = '0x3de27efa2f1aa663ae5d458857e731c129069f29'
order by block_hour desc;


with balancer_pool_liquidity_latest_per_day as (
  select * 
    , row_number() over (partition by cast(block_hour as date) order by token_symbol) as rn
  from (
    select *
      , rank() over (partition by cast(block_hour as date) order by block_hour desc) as rnk
      from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` 
      -- from {{ source('raw_data', 'common_balancer_pool_liquidity') }}
      where pool_address = '0x3de27efa2f1aa663ae5d458857e731c129069f29' --20wstETH-80AAVE
      order by block_hour desc
  ) where rnk = 1
  order by block_hour desc
)
, initial_deposit AS (
  SELECT 
    block_hour
    , total_liquidity AS initial_pool_tvl
    , total_shares AS initial_pool_num_lp_tokens
    , 100000.0 AS user_deposit_amount
    -- Calculate the user's LP tokens received on deposit
    , 100000.0 * (total_shares / total_liquidity) AS user_lp_tokens
    -- Get the non-lp user's AAVE and wstETH holdings
    , 80000/(select token_latest_usd_price from balancer_pool_liquidity_latest_per_day where cast(block_hour as date) = '2024-02-11' and token_address = '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9') as non_lp_user_aave_token_balance
    , 20000/(select token_latest_usd_price from balancer_pool_liquidity_latest_per_day where cast(block_hour as date) = '2024-02-11' and token_address = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0') as non_lp_user_wsteth_token_balance
  FROM balancer_pool_liquidity_latest_per_day
  WHERE cast(block_hour as date) = '2024-02-11'
  LIMIT 1
)
, user_calculations as (
  select 
    bpl1.block_hour
    , bpl1.total_liquidity as pool_tvl
    , bpl1.total_shares as pool_num_lp_tokens
    -- AAVE data
    , bpl1.token_latest_usd_price as aave_usd_price
    , bpl1.token_balance as pool_aave_token_balance
    , bpl1.token_balance * bpl1.token_latest_usd_price as pool_aave_value
    -- wstETH data
    , bpl2.token_latest_usd_price as wsteth_usd_price
    , bpl2.token_balance as pool_wsteth_token_balance
    , bpl2.token_balance * bpl2.token_latest_usd_price as pool_wsteth_value
    -- user data
    , id.user_deposit_amount
    , id.user_lp_tokens
    , id.user_lp_tokens / bpl1.total_shares as user_share
    , (id.user_lp_tokens / bpl1.total_shares) * bpl1.token_balance as lp_user_aave_token_balance
    , id.non_lp_user_aave_token_balance 
    , (id.user_lp_tokens / bpl1.total_shares) * bpl1.token_balance * bpl1.token_latest_usd_price as lp_user_aave_value
    , id.non_lp_user_aave_token_balance * bpl1.token_latest_usd_price as non_lp_user_aave_value
    , (id.user_lp_tokens / bpl1.total_shares) * bpl2.token_balance as lp_user_wsteth_token_balance
    , id.non_lp_user_wsteth_token_balance
    , (id.user_lp_tokens / bpl1.total_shares) * bpl2.token_balance * bpl2.token_latest_usd_price as lp_user_wsteth_value
    , id.non_lp_user_wsteth_token_balance * bpl2.token_latest_usd_price as non_lp_user_wsteth_value
  from (select * from balancer_pool_liquidity_latest_per_day where token_address = '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9') bpl1 --AAVE
  left join (select * from balancer_pool_liquidity_latest_per_day where token_address = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0') bpl2 --wstETH
    on bpl1.rn = bpl2.rn-1
    and bpl1.block_hour = bpl2.block_hour
  cross join initial_deposit id
  where bpl2.rn is not null
  order by bpl1.block_hour
)
select 
  *
  , lp_user_aave_value + lp_user_wsteth_value as lp_user_total_value
  , non_lp_user_aave_value + non_lp_user_wsteth_value as non_lp_user_total_value
  , round((((lp_user_aave_value + lp_user_wsteth_value)/(non_lp_user_aave_value + non_lp_user_wsteth_value)) - 1) * 100, 4) as manual_il
from user_calculations 
where cast(block_hour as date) > '2024-02-10'
order by block_hour desc



select * from tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis;