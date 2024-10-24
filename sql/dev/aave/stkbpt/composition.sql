
with ins as (
  select
    `to` as address
    , 'in' as action
    , sum(value) as num_stk_lp_tokens
  from datamart_aave.aave_stk_token_transfers_bigquery
  --  from {{ ref('aave_stk_token_transfers_bigquery') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
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
  group by `from`
)
, both as (
  select * from ins where address <> '0x0000000000000000000000000000000000000000' --filter the null address out as they cancel each other 
  union all
  select * from outs where address <> '0x0000000000000000000000000000000000000000'
)
, composition_percentage as (
  select 
    b.address
    , sum(num_stk_lp_tokens) as num_stk_lp_tokens
    , (select 
        sum(case 
              when `from` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
              else 0
            end)
          - sum(case 
              when `to` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
              else 0 
            end) as total_num_stk_lp_tokens
      from datamart_aave.aave_stk_token_transfers_bigquery
      -- from {{ ref('aave_stk_token_transfers_bigquery') }}
      where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101') as total_num_stk_lp_tokens
    , sum(num_stk_lp_tokens) / (select 
                                sum(case 
                                      when `from` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
                                      else 0
                                    end)
                                  - sum(case 
                                      when `to` = '0x0000000000000000000000000000000000000000' then cast(value as numeric)
                                      else 0 
                                    end) as total_num_stk_lp_tokens
                                from datamart_aave.aave_stk_token_transfers_bigquery
                                -- from {{ ref('aave_stk_token_transfers_bigquery') }}
                                where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101') * 100 as perc
  from both b
  group by b.address
  order by num_stk_lp_tokens desc
)
, cooldown_initiations as (
  SELECT 
    block_timestamp
    , user
    , amount
  from events.aave_stk_token_cooldown
  -- FROM {{ source('events','aave_stk_token_cooldown') }} 
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  order by user, block_timestamp asc
)
, first_deposit as (
  SELECT 
    `to` as user
    , min(block_day) as first_mint
  from datamart_aave.aave_stk_token_transfers_bigquery
  -- FROM {{ ref('aave_stk_token_transfers_bigquery') }}
  WHERE `to` in (select user from cooldown_initiations)
  and stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  group by user
)
, days_in_cooldown as (
  select
    fd.user
    , count(ci.block_timestamp) as number_of_initiations
    , count(ci.block_timestamp) * 20 as days_in_cooldown
  from first_deposit fd
  left join cooldown_initiations ci on fd.user = ci.user
  group by fd.user
)
select 
  cp.*
  , di.days_in_cooldown
  , coalesce(di.days_in_cooldown / DATE_DIFF(CURRENT_DATE(), DATE(fd.first_mint), DAY) * 100, 0) as percentage_of_time_spent_in_cooldown
from composition_percentage cp
left join first_deposit fd on cp.address = fd.user
left join days_in_cooldown di on cp.address = di.user
where cp.num_stk_lp_tokens <> 0
order by cp.perc desc
;



select * from datamart_aave.aave_stkbpt_composition;