

select * from datamart_aave.aave_stkbpt_compounding_analysis order by date desc;


with recursive cte as (
  (
    select 
      cast(inv.block_hour as date) as date
      , apr.num_stk_lp_tokens_total
      , apr.tvs 
      , inv.pool_tvl
      , inv.pool_num_lp_tokens
      , apr.usd_emission_per_day
      -- no compounding
      -- , inv.user_share as user_lp_share_no_compounding
      , inv.user_lp_tokens as user_lp_tokens_no_compounding
      , inv.user_lp_tokens / inv.pool_num_lp_tokens as user_lp_share_no_compounding
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_no_compounding
      , inv.lp_user_aave_token_balance as aave_token_balance_no_compounding
      , inv.lp_user_aave_token_balance * inv.aave_usd_price as aave_usd_value_no_compounding
      -- daily compounding
      , inv.user_lp_tokens as user_lp_tokens_1_day
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_1_day
      , inv.user_lp_tokens / inv.pool_num_lp_tokens as user_lp_share_1_day
      , inv.lp_user_aave_token_balance as aave_token_balance_1_day
      , inv.lp_user_aave_value as aave_usd_value_1_day
      , inv.lp_user_wsteth_token_balance as wsteth_token_balance_1_day
      , inv.lp_user_wsteth_value as wsteth_usd_value_1_day
      , inv.lp_user_aave_value + inv.lp_user_wsteth_value as user_usd_total_value_1_day
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_1_day
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_1_day
      -- compounding every 7 days
      , inv.user_lp_tokens as user_lp_tokens_7_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_7_days
      , inv.lp_user_aave_token_balance as aave_token_balance_7_days
      , inv.lp_user_aave_value as aave_usd_value_7_days
      , inv.lp_user_wsteth_token_balance as wsteth_token_balance_7_days
      , inv.lp_user_wsteth_value as wsteth_usd_value_7_days
      , inv.lp_user_aave_value + inv.lp_user_wsteth_value as user_usd_total_value_7_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_7_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_7_days
      , CAST(1 AS INT64) as day_counter_7_days
      , CAST(0 AS FLOAT64) as accumulated_lp_tokens_7_days
      -- compounding every 14 days (fortnightly)
      , inv.user_lp_tokens as user_lp_tokens_14_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_14_days
      , inv.lp_user_aave_token_balance as aave_token_balance_14_days
      , inv.lp_user_aave_value as aave_usd_value_14_days
      , inv.lp_user_wsteth_token_balance as wsteth_token_balance_14_days
      , inv.lp_user_wsteth_value as wsteth_usd_value_14_days
      , inv.lp_user_aave_value + inv.lp_user_wsteth_value as user_usd_total_value_14_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_14_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_14_days
      , CAST(1 AS INT64) as day_counter_14_days
      , CAST(0 AS FLOAT64) as accumulated_lp_tokens_14_days
      -- compounding every 30 days (monthly)
      , inv.user_lp_tokens as user_lp_tokens_30_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_30_days
      , inv.lp_user_aave_token_balance as aave_token_balance_30_days
      , inv.lp_user_aave_value as aave_usd_value_30_days
      , inv.lp_user_wsteth_token_balance as wsteth_token_balance_30_days
      , inv.lp_user_wsteth_value as wsteth_usd_value_30_days
      , inv.lp_user_aave_value + inv.lp_user_wsteth_value as user_usd_total_value_30_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_30_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_30_days
      , CAST(1 AS INT64) as day_counter_30_days
      , CAST(0 AS FLOAT64) as accumulated_lp_tokens_30_days
      -- compounding every 90 days (quarterly)
      , inv.user_lp_tokens as user_lp_tokens_90_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_90_days
      , inv.lp_user_aave_token_balance as aave_token_balance_90_days
      , inv.lp_user_aave_value as aave_usd_value_90_days
      , inv.lp_user_wsteth_token_balance as wsteth_token_balance_90_days
      , inv.lp_user_wsteth_value as wsteth_usd_value_90_days
      , inv.lp_user_aave_value + inv.lp_user_wsteth_value as user_usd_total_value_90_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_90_days
      , inv.user_lp_tokens / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_90_days
      , CAST(1 AS INT64) as day_counter_90_days
      , CAST(0 AS FLOAT64) as accumulated_lp_tokens_90_days
    from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr_historical` apr
    -- from {{ ref('aave_stkbpt_apr_historical') }} apr
    left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv 
    -- left join {{ ref('aave_stkbpt_investment_analysis') }} inv 
      on date_add(apr.date, interval 1 day) = cast(inv.block_hour as date)
    where cast(inv.block_hour as date) = '2024-02-11' 
      and apr.date = '2024-02-10'
  )

  union all 

  select
    date_add(c.date, interval 1 day) as date
    , apr.num_stk_lp_tokens_total
    , apr.tvs
    , inv.pool_tvl
    , inv.pool_num_lp_tokens
    , apr.usd_emission_per_day
    -- no compounding
    -- , inv.user_share as user_lp_share_no_compounding
    , inv.user_lp_tokens as user_lp_tokens_no_compounding
    , inv.user_lp_tokens / inv.pool_num_lp_tokens as user_lp_share_no_compounding
    , inv.user_lp_tokens / apr.num_stk_lp_tokens_total as user_stkbpt_share_no_compounding
    , inv.lp_user_aave_token_balance as aave_token_balance_no_compounding
    , inv.lp_user_aave_token_balance * inv.aave_usd_price as aave_usd_value_no_compounding
    -- daily compounding
    , c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day as user_lp_tokens_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / apr.num_stk_lp_tokens_total as user_stkbpt_share_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens as user_lp_share_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance as aave_token_balance_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price as aave_usd_value_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance as wsteth_token_balance_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price as wsteth_usd_value_1_day
    , ((c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + ((c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price) as user_usd_total_value_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward_1_day
    , (c.user_lp_tokens_1_day + c.lp_tokens_daily_reward_1_day) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total as lp_tokens_daily_reward_1_day
    -- compounding every 7 days
    , case
        when c.day_counter_7_days = 7 then c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days -- reinvest on the 7th day
        else c.user_lp_tokens_7_days
        end as user_lp_tokens_7_days
    , case
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_7_days / apr.num_stk_lp_tokens_total
        end as user_stkbpt_share_7_days
    , case 
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        else c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        end as aave_token_balance_7_days
    , case 
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens* inv.pool_aave_token_balance * inv.aave_usd_price
        else c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        end as aave_usd_value_7_days
    , case 
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        else c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        end as wsteth_token_balance_7_days
    , case 
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        else c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        end as wsteth_usd_value_7_days
    , case 
        when c.day_counter_7_days = 7 then ((c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + ((c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        else (c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + (c.user_lp_tokens_7_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        end as user_usd_total_value_7_days
    , case
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        else c.user_lp_tokens_7_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        end as usd_daily_reward_7_days
    , case
        when c.day_counter_7_days = 7 then (c.user_lp_tokens_7_days + c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_7_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        end as lp_tokens_daily_reward_7_days
    , case
        when c.day_counter_7_days = 7 then CAST(1 AS INT64)
        else c.day_counter_7_days + 1
        end as day_counter_7_days
    , case
        when c.day_counter_7_days < 7 then c.accumulated_lp_tokens_7_days + c.lp_tokens_daily_reward_7_days
        else CAST(0 AS FLOAT64)
        end as accumulated_lp_tokens_7_days
    -- compounding every 14 days (fortnightly)
    , case
        when c.day_counter_14_days = 14 then c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days
        else c.user_lp_tokens_14_days
        end as user_lp_tokens_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_14_days / apr.num_stk_lp_tokens_total
        end as user_stkbpt_share_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        else c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        end as aave_token_balance_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        else c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        end as aave_usd_value_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        else c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        end as wsteth_token_balance_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        else c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        end as wsteth_usd_value_14_days
    , case
        when c.day_counter_14_days = 14 then ((c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + ((c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        else (c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + (c.user_lp_tokens_14_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        end as user_usd_total_value_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        else c.user_lp_tokens_14_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        end as usd_daily_reward_14_days
    , case
        when c.day_counter_14_days = 14 then (c.user_lp_tokens_14_days + c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_14_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        end as lp_tokens_daily_reward_14_days
    , case
        when c.day_counter_14_days = 14 then CAST(1 AS INT64)
        else c.day_counter_14_days + 1
        end as day_counter_14_days
    , case
        when c.day_counter_14_days < 14 then c.accumulated_lp_tokens_14_days + c.lp_tokens_daily_reward_14_days
        else CAST(0 AS FLOAT64)
        end as accumulated_lp_tokens_14_days
    -- compounding every 30 days (monthly)
    , case
        when c.day_counter_30_days = 30 then c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days
        else c.user_lp_tokens_30_days
        end as user_lp_tokens_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_30_days / apr.num_stk_lp_tokens_total
        end as user_stkbpt_share_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        else c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        end as aave_token_balance_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        else c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        end as aave_usd_value_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        else c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        end as wsteth_token_balance_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        else c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        end as wsteth_usd_value_30_days
    , case
        when c.day_counter_30_days = 30 then ((c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + ((c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        else (c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + (c.user_lp_tokens_30_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        end as user_usd_total_value_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        else c.user_lp_tokens_30_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        end as usd_daily_reward_30_days
    , case
        when c.day_counter_30_days = 30 then (c.user_lp_tokens_30_days + c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_30_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        end as lp_tokens_daily_reward_30_days
    , case
        when c.day_counter_30_days = 30 then CAST(1 AS INT64)
        else c.day_counter_30_days + 1
        end as day_counter_30_days
    , case
        when c.day_counter_30_days < 30 then c.accumulated_lp_tokens_30_days + c.lp_tokens_daily_reward_30_days
        else CAST(0 AS FLOAT64)
        end as accumulated_lp_tokens_30_days
    -- compounding every 90 days (quarterly)
    , case
        when c.day_counter_90_days = 90 then c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days
        else c.user_lp_tokens_90_days
        end as user_lp_tokens_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_90_days / apr.num_stk_lp_tokens_total
        end as user_stkbpt_share_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        else c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance
        end as aave_token_balance_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        else c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price
        end as aave_usd_value_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        else c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance
        end as wsteth_token_balance_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        else c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price
        end as wsteth_usd_value_90_days
    , case
        when c.day_counter_90_days = 90 then ((c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + ((c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        else (c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_aave_token_balance * inv.aave_usd_price) + (c.user_lp_tokens_90_days / inv.pool_num_lp_tokens * inv.pool_wsteth_token_balance * inv.wsteth_usd_price)
        end as user_usd_total_value_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        else c.user_lp_tokens_90_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day
        end as usd_daily_reward_90_days
    , case
        when c.day_counter_90_days = 90 then (c.user_lp_tokens_90_days + c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days) / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        else c.user_lp_tokens_90_days / apr.num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.num_stk_lp_tokens_total
        end as lp_tokens_daily_reward_90_days
    , case
        when c.day_counter_90_days = 90 then CAST(1 AS INT64)
        else c.day_counter_90_days + 1
        end as day_counter_90_days
    , case
        when c.day_counter_90_days < 90 then c.accumulated_lp_tokens_90_days + c.lp_tokens_daily_reward_90_days
        else CAST(0 AS FLOAT64)
        end as accumulated_lp_tokens_90_days
  from cte c
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr_historical` apr 
  -- left join {{ ref('aave_stkbpt_apr_historical') }} apr
    on c.date = apr.date
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv 
  -- left join {{ ref('aave_stkbpt_investment_analysis') }} inv 
    on date_add(c.date, interval 1 day) = cast(inv.block_hour as date)
  where c.date < '2024-03-10'
--   where c.date < current_date()
)
select * from cte order by date
;


select * from tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis;
select * from tokenlogic-data.datamart_aave.aave_stkbpt_compounding_analysis;
select * from tokenlogic-data-dev.datamart_aave.aave_stkbpt_compounding_analysis;
select * from tokenlogic-data.datamart_aave.aave_stkbpt_compounding_analysis;