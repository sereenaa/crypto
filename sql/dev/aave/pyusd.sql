select * from raw_data_aave.chainhour_protocol_data limit 5;
select distinct chain, market, reserve, symbol from raw_data_aave.chainhour_protocol_data order by 1,2,4;
-- aEthPYUSD 0x6c3ea9036406852006290770bedfcaba0e23a0e8
select * from raw_data_aave.chainhour_protocol_data where reserve = '0x6c3ea9036406852006290770bedfcaba0e23a0e8' and symbol = 'aEthPYUSD' order by block_hour desc limit 100;
select 
  block_hour
  , liquidity_rate
from raw_data_aave.chainhour_protocol_data where reserve = '0x6c3ea9036406852006290770bedfcaba0e23a0e8' and symbol = 'aEthPYUSD' order by block_hour desc limit 100;

select * from raw_data_aave.aave_ethereum_token_transfers_backfill where address = '0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e';
select * from raw_data_aave.aave_ethereum_token_transfers where address = '0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e';


-- insert into tokenlogic-data.raw_data_aave.aave_ethereum_token_transfers
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
-- from tokenlogic-data.raw_data_aave.aave_ethereum_token_transfers_backfill
-- where address = '0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e'
-- ;



with deposits as (
  select
    block_timestamp
    , to_address as user
    -- , cast(quantity as float64) / 1000000 as amount
    , cast(quantity as bignumeric) as amount
  from raw_data_aave.aave_ethereum_token_transfers 
  where address = '0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e' -- aEthPYUSD
    and from_address = '0x0000000000000000000000000000000000000000'
)
, withdrawals as (
  select
    block_timestamp
    , from_address as user
    -- , cast(quantity as float64) / 1000000 * -1 as amount
    , cast(quantity as bignumeric) * -1 as amount
  from raw_data_aave.aave_ethereum_token_transfers 
  where address = '0x0c0d01abf3e6adfca0989ebba9d6e85dd58eab1e' -- aEthPYUSD
    and to_address = '0x0000000000000000000000000000000000000000'
)
, totals as (
  select * from deposits 
  union all 
  select * from withdrawals 
  order by user, block_timestamp
)
, cumulative_totals as (
  select
    block_timestamp 
    , user
    , sum(amount) over (partition by user order by block_timestamp) as cumulative_amount
    , row_number() over (partition by user order by block_timestamp desc) as row_num
  from totals
  where user = '0x01820d92f8f86947ca0454789172ad60e05817fa'
  order by block_timestamp
)
-- , september_holding_users as (
--   select 
--     distinct user
--   from cumulative_totals 
--   where row_num = 1
--     and block_timestamp between timestamp('2024-09-01 00:00:00') and timestamp('2024-09-30 23:59:59')
--     and cumulative_amount > 0
-- )
, user_daily_balance as (
  select 
    cpd.block_hour
    , last_value(ct.user ignore nulls) over (order by cpd.block_hour) as user
    , last_value(ct.cumulative_amount ignore nulls) over (order by cpd.block_hour) as balance
    , cpd.liquidity_rate
  from raw_data_aave.chainhour_protocol_data cpd
  left join cumulative_totals ct on timestamp_trunc(ct.block_timestamp, hour) = cpd.block_hour
  where cpd.reserve = '0x6c3ea9036406852006290770bedfcaba0e23a0e8' 
    and cpd.symbol = 'aEthPYUSD'
  order by cpd.block_hour
)
, daily_interest as (
  select 
    block_hour
    , user
    , cast(balance as float64) / 1000000 as balance
    , liquidity_rate
    , cast(balance as float64) / 1000000 * liquidity_rate / 365 as daily_interest_rate
  from user_daily_balance
  where user is not null
)
, compounded_rates as (
  select 
    block_hour
    , user
    , balance
    , liquidity_rate
    , daily_interest_rate
    , EXP(SUM(cast(LOG(1 + liquidity_rate) as float64)) OVER (ORDER BY block_hour)) - 1 AS cumulative_compound_return
  from daily_interest
  order by block_hour
)
select 
  block_hour
  , user
  , balance
  , liquidity_rate
  , daily_interest_rate
  , cumulative_compound_return
  , balance * cumulative_compound_return / 365 as new_balance
from compounded_rates
;

-- 0x78f8bd884c3d738b74b420540659c82f392820e0 large negative cumulative total
-- 0x01820d92f8f86947ca0454789172ad60e05817fa good example


select * from raw_data.gho_chainhour_aave_collateral where chain = "ethereum" order by user_address, block_hour limit 500;
select * from raw_data.common_chainhour_block_numbers order by block_hour desc limit 500;
-- insert into tokenlogic-data-dev.raw_data.common_chainhour_block_numbers
-- select * from tokenlogic-data.raw_data.common_chainhour_block_numbers 
-- where block_hour > (select max(block_hour) from tokenlogic-data-dev.raw_data.common_chainhour_block_numbers);
select * from raw_data_aave.aave_pyusd_chainhour_collateral order by user_address, block_hour limit 50;
select distinct block_hour from raw_data.aave_pyusd_chainhour_collateral;
select count(*) from raw_data.aave_pyusd_chainhour_collateral;
select * 
from raw_data_aave.aave_pyusd_chainhour_collateral 
where block_hour between timestamp()
order by user_address, block_hour;
-- drop table raw_data_aave.aave_pyusd_chainhour_collateral;
select * from raw_data_aave.aave_pyusd_chainhour_collateral order by user_address, block_hour, reserve_symbol;
select * from raw_data.gho_chainhour_gho_oracle_prices where reserve = '0x6c3ea9036406852006290770bedfcaba0e23a0e8' order by block_hour;

select 
  c.block_hour
  , c.user_address
  , c.deposit_balance
  , p.price_usd
  , c.deposit_balance * p.price_usd as deposit_usd
  , if(c.emode_category is null, c.liquidation_threshold, c.emode_liquidation_threshold) as liquidation_threshold
  , c.variable_debt
  , c.variable_debt * p.price_usd as variable_debt_usd
  , c.liquidity_rate 
from raw_data_aave.aave_pyusd_chainhour_collateral c 
left join raw_data_aave.gho_chainhour_gho_oracle_prices p on c.block_hour = p.block_hour
where c.reserve_symbol = 'PYUSD'
  and p.reserve = '0x6c3ea9036406852006290770bedfcaba0e23a0e8' -- PYUSD
  and c.user_address = '0x067da064491c2bc0b1d9bfcf425444ff124451e3'
order by c.user_address, c.block_hour
;



with initial_supply_liquidity_index as (
  select block_hour, user_address, liquidity_index from (
    select
      block_hour
      , user_address
      , liquidity_index
      , row_number() over (partition by user_address order by block_hour) as rn
    from raw_data_aave.aave_pyusd_chainhour_collateral
  -- from {{ source('raw_data_aave', 'aave_pyusd_chainhour_collateral') }}
    where reserve_symbol = 'PYUSD'
  ) where rn = 1
)
, aTokens as (
  select
    c.block_hour
    , c.user_address
    , c.deposit_balance -- aEthPYUSD deposit amount (static number of tokens)
    , c.liquidity_index
    , s.liquidity_index as initial_supply_liquidity_index
    , c.deposit_balance * (c.liquidity_index/s.liquidity_index) as aEthPYUSD_value
    , c.variable_debt
    , if(c.emode_category is null, c.liquidation_threshold, c.emode_liquidation_threshold) as liquidation_threshold
    , (c.deposit_balance * (c.liquidity_index/s.liquidity_index)) - (c.variable_debt / coalesce(nullif(if(c.emode_category is null, c.liquidation_threshold, c.emode_liquidation_threshold), 0), (c.deposit_balance * (c.liquidity_index/s.liquidity_index)))) as eligible_holding
  from raw_data_aave.aave_pyusd_chainhour_collateral c
  -- from {{ source('raw_data_aave', 'aave_pyusd_chainhour_collateral') }} c
  left join initial_supply_liquidity_index s on c.user_address = s.user_address
  where c.reserve_symbol = 'PYUSD'
  order by c.user_address, c.block_hour
)
, extra_rewards as (
  select 
    block_hour
    , user_address
    , deposit_balance
    , liquidity_index/initial_supply_liquidity_index as liquidity_index
    , aEthPYUSD_value
    , eligible_holding
    , case 
        when extract(day from block_hour) = 1 and extract(hour from block_hour) = 0 then 0.04 * eligible_holding
        else null
        end as monthly_incentive
  from aTokens
)
select 
  cast(block_hour as date) as block_day
  , user_address
  , deposit_balance
  , liquidity_index
  , aEthPYUSD_value
  , aEthPYUSD_value - deposit_balance as organic_yield_accrued
  , eligible_holding
  , last_value(monthly_incentive ignore nulls) over (partition by user_address order by block_hour) as monthly_incentive
  , sum(monthly_incentive) over (partition by user_address order by block_hour) as total_cumulative_incentives
from extra_rewards
where extract(hour from block_hour) = 0
order by user_address, block_hour
;





select * from datamart_aave.aave_pyusd_rate where block_day between '2024-09-01' and '2024-09-30' order by user_address, block_day;

with cte as (
  select * 
    , row_number() over (partition by user_address order by block_day desc) as rn
  from datamart_aave.aave_pyusd_rate where block_day between '2024-09-01' and '2024-09-30' order by user_address, block_day
) 
select * from cte where rn = 1 and block_day <> '2024-09-30';



select * from datamart_gearbox.gearbox_deposit_borrow_rate_hour;

select * from raw_data_aave.aave_pyusd_chainhour_collateral;
select * from raw_data_aave.aave_pyusd_chainhour_collateral where user_address = '0x067da064491c2bc0b1d9bfcf425444ff124451e3' order by block_hour;


select * from datamart_aave.aave_pyusd_rate where user_address = '0xaddb38203d5cc95b46aa690f94cb12fece0f9e8e';
select * from datamart_aave.aave_pyusd_rate where user_address = '0x3cf59f2be573ad24c0ea5763d8ae88fbd5134a6f';
select * from datamart_aave.aave_pyusd_rate where user_address = '0x067da064491c2bc0b1d9bfcf425444ff124451e3';
select * from datamart_aave.aave_pyusd_rate where user_address = '0xea14c187d97cb61aa35acca490c361a5ce7b4ace';
select * from datamart_aave.aave_pyusd_rate where user_address = '0xcc2ca22aaefe22a0144a0260731a40a725affff0';
select * from datamart_aave.aave_pyusd_rate where user_address = '0xea14c187d97cb61aa35acca490c361a5ce7b4ace';