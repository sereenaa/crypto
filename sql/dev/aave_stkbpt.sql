-- SELECT 
--   --*
--   contract_address
-- FROM `tokenlogic-data-dev.indexer_prod.gsm_buy_token` 
-- -- where tx_hash = '0x0ccf10d1caad71b161e3c671c37cee15d6121c1807357d8138a751cb73c12e19'
-- where tx_hash = '0xd825bd080829a9dcdc5f12dd8d7f108ea5ba965f1a50b27c04988301c348fa22'
-- LIMIT 1000


-- select * from `tokenlogic-data-dev.indexer_prod.gsm_buy_token` 
-- where contract_address = '0x686f8d21520f4ecec7ba577be08354f4d1eb8262'
-- limit 1

-- select * from `tokenlogic-data-dev.datamart_gho.gho_gsm_ui_latest_20_txs`

-- select * from `tokenlogic-data-dev.raw_data.gho_chainhour_gho_reserve_data` limit 100

-- select * from `tokenlogic-data-dev.events.gho_facilitator_bucket_level_updated`
-- select * from `tokenlogic-data-dev.events.gho_facilitator_bucket_capacity_updated`

-- select * from `tokenlogic-data-dev.events.aave_stk_token_transfer` where "from" = '0x25F2226B597E8F9514B3F68F00f494cF4f286491' limit 1000;
select * from `tokenlogic-data-dev.events.aave_stk_token_redeem` 
-- where "from" = '0x25F2226B597E8F9514B3F68F00f494cF4f286491' 
limit 1000;

--staked token contract addresses
--0x4da27a545c0c5b758a6ba100e3a049001de870f5 stkAAVE
--0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d stkGHO
--0x9eda81c21c273a82be9bbc19b6a6182212068101 stkAAVEwstETHBPTv2 
--0xa1116930326d21fb917d5a27f1e9943a9595fb47 stkABPT 

--0x25F2226B597E8F9514B3F68F00f494cF4f286491 aave ecosystem reserve
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued` order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued` where tx_hash = '0xa6d10ca36fcfb4858edc62d6ff1c59d77aec2dad2642ce66def7b3ea7e5f66e0' order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued` where tx_hash = '0x7ceeb68f65fb782b9fa38e894ef46de0be1a1b21a09b4af2ce487128a1df6809' order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued` where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101';
select min(block_timestamp) from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued`;
select distinct(contract_address) from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_accrued`;


select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_claimed` order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_transfer` order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_transfer` where tx_hash = '0xda4f8717210e65785dd82415c88a5362363b44606a6dfed72719d304b8fe9eb8';
select distinct(symbol) from `tokenlogic-data-dev.indexer_prod.stk_token_transfer`;
select min(date(timestamp_seconds(block_timestamp))) from `tokenlogic-data-dev.indexer_prod.stk_token_transfer`;

select * from `tokenlogic-data-dev.indexer_prod.stk_token_staked` where contract_address in ('0x9eda81c21c273a82be9bbc19b6a6182212068101', '0xa1116930326d21fb917d5a27f1e9943a9595fb47') order by block_timestamp desc limit 100;
--0xd26a05d386fc6f0e2180edec6a2cbae82abad854bda25726da3e65f4a74f4432
select * from `tokenlogic-data-dev.indexer_prod.stk_token_staked` where contract_address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' order by block_timestamp desc limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_staked` where assets_raw <> shares_raw limit 5;
select distinct(contract_address) from `tokenlogic-data-dev.indexer_prod.stk_token_staked`;

select * from `tokenlogic-data-dev.indexer_prod.stk_token_redeem` limit 10;
--0xda4f8717210e65785dd82415c88a5362363b44606a6dfed72719d304b8fe9eb8
select distinct(contract_address) from `tokenlogic-data-dev.indexer_prod.stk_token_redeem`;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_redeem` where contract_address in ('0x9eda81c21c273a82be9bbc19b6a6182212068101', '0xa1116930326d21fb917d5a27f1e9943a9595fb47') limit 100;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_redeem` where contract_address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' and date(timestamp_seconds(block_timestamp)) = '2023-10-27'; 
-- 2023-07-06 0xc71af1a32296fb9ac1266d4232f3f4d410fe615b8a071dc1c3694b4098ed2120
-- 2023-10-27 0x530df37e8189daed6721cec9a3b38833a2266fbd06438f30b8b70b6d06f05b59 --50,000,000 stkABPT tokens were unstaked

select * from `tokenlogic-data-dev.indexer_prod.stk_token_transfer` where contract_address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' and `to` = '0x0000000000000000000000000000000000000000'; 
select * from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' order by block_day desc limit 100;
select * from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_day desc limit 100; --stkAAVEwstETHBPTv2
select min(block_day) from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101';
select min(block_day) from `tokenlogic-data-dev.events.aave_stk_token_transfer`;

select * from `tokenlogic-data-dev.raw_data_aave.aave_chainhour_oracle_prices` limit 5;
select distinct(market) from `tokenlogic-data-dev.raw_data_aave.aave_chainhour_oracle_prices`;
select * from `tokenlogic-data-dev.prices.chainlink_subgraph_prices` limit 100;
select distinct(pair) from `tokenlogic-data-dev.prices.chainlink_subgraph_prices`;
select 
  cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) as date
  , price
from (
  select 
    block_timestamp
    , price
    , row_number() over (partition by cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) order by block_timestamp desc) as row_num
  from `tokenlogic-data-dev.prices.chainlink_subgraph_prices` 
  where pair = 'AAVE/USD'
) where row_num = 1
order by date
;

select * from `tokenlogic-data-dev.raw_data_aave.aave_market_tokens` limit 100;
select distinct(atoken_symbol) from `tokenlogic-data-dev.raw_data_aave.aave_market_tokens`;
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` limit 100;
select distinct(name) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` order by name;
select * from `tokenlogic-data-dev.indexer_prod.a_tokenv2_mint_event` limit 100;
select * from `tokenlogic-data-dev.indexer_prod.a_tokenv2_mint_event` where contract_address = '0x3de27EFa2F1AA663Ae5D458857e731c129069F29' limit 100;
select distinct(market) from `tokenlogic-data-dev.indexer_prod.a_tokenv2_mint_event`;
select * from `tokenlogic-data-dev.indexer_prod.a_tokenv2_mint_event` where market = 'ethereum_v2' limit 100;

select * from `tokenlogic-data-dev.datamart_gho.stkgho_ui_apr_time` order by block_day;
select * from `tokenlogic-data-dev.raw_data.angle_merit_apy_endpoint` limit 100;
select * from `tokenlogic-data-dev.datamart_gho.stkgho_mints_day` limit 100;
select * from `tokenlogic-data-dev.datamart_gho.gho_balancer_pools_apr` limit 100;
select * from `tokenlogic-data-dev.raw_data.gho_chainhour_gho_transfers` limit 100;
select distinct(symbol) from `tokenlogic-data-dev.raw_data.gho_chainhour_gho_transfers` limit 100;
select * from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` limit 100;
select * from `tokenlogic-data-dev.raw_data.common_chainhour_erc20_supply` limit 100;
select * from `tokenlogic-data-dev.raw_data.common_chainhour_erc20_supply` where address = '0x9eda81c21c273a82be9bbc19b6a6182212068101';
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` limit 100;
select distinct(symbol) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` order by symbol;
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' order by block_hour desc limit 1000;
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' and total_shares is not null;
select distinct(swap_fee) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE';
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20GHO-80PSP';
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' and total_shares is not null and total_swap_volume is not null;




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
, aave_prices as (
  select 
    cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) as date
    , price
  from (
    select 
      block_timestamp
      , price
      , row_number() over (partition by cast(FORMAT_TIMESTAMP('%Y-%m-%d', block_timestamp) as date) order by block_timestamp desc) as row_num
    from `tokenlogic-data.prices.chainlink_subgraph_prices` 
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
    from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` 
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
  from `tokenlogic-data.raw_data.common_balancer_apiv3_results`,
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
left join `tokenlogic-data.external_tables.balancer_swap_fees_historical` ash on ej.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', ash.block_hour) as date)+1
-- left join external_tables.balancer_swap_fees_historical ash on ej.date = cast(FORMAT_TIMESTAMP('%Y-%m-%d', ash.block_hour) as date)
where ej.date >= '2024-02-10'
order by ej.date desc
--;



select * from `tokenlogic-data-dev.external_tables.balancer_swap_fees_historical` limit 100;
select * from `tokenlogic-data-dev.raw_data.balancer_swap_fees_historical` limit 100;
select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` order by date desc;
select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` where tvs > tvl_pool order by date desc;
select * from `tokenlogic-data.datamart_aave.aave_stkbpt_apr` order by date desc;

select * from `tokenlogic-data-dev.datamart_gho.stkgho_emissions` limit 5;
select * from `tokenlogic-data-dev.events.aave_stk_token_asset_config_updated`;

--earliest date is '2023-11-28'
select 
  cast(block_timestamp as date) as date
  , asset
  , asset_symbol
  , emission_per_day
from `tokenlogic-data-dev.events.aave_stk_token_asset_config_updated`
where asset in ('0x9eda81c21c273a82be9bbc19b6a6182212068101')
  and emission_per_day <> 0.0
order by date
;


with ins as (
  select
    `to` as address
    , 'in' as action
    , sum(value) as num_stk_lp_tokens
  from `tokenlogic-data-dev.events.aave_stk_token_transfer`
  --  from {{ source('events', 'aave_stk_token_transfer') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
  group by `to`
)
, outs as (
  select
    `from` as address
    , 'out' as action
    , sum(value)*-1 as num_stk_lp_tokens
  from `tokenlogic-data-dev.events.aave_stk_token_transfer`
  -- from {{ source('events', 'aave_stk_token_transfer') }}
  where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  group by `from`
)
, both as (
  select * from ins where address <> '0x0000000000000000000000000000000000000000' --filter the null address out as they cancel each other 
  union all
  select * from outs where address <> '0x0000000000000000000000000000000000000000'
)
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
    from `tokenlogic-data-dev.events.aave_stk_token_transfer`
    -- from {{ source('events', 'aave_stk_token_transfer') }}
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
                              from `tokenlogic-data-dev.events.aave_stk_token_transfer`
                              -- from {{ source('events', 'aave_stk_token_transfer') }}
                              where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101') * 100 as perc
from both b
group by b.address
order by num_stk_lp_tokens desc
--;


select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_composition`;
select count(*) from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_composition`;





-- Schema change
-- CREATE OR REPLACE TABLE `tokenlogic-data.raw_data.common_balancer_pool_liquidity_backup`
-- AS SELECT * FROM `tokenlogic-data.raw_data.common_balancer_pool_liquidity`;


-- DROP TABLE `tokenlogic-data.raw_data.common_balancer_pool_liquidity`;


-- select count(*) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity_backup`;



-- INSERT INTO `tokenlogic-data.raw_data.common_balancer_pool_liquidity`
-- SELECT
--   block_hour,
--   chain,
--   pool_id,
--   pool_address,
--   base_token,
--   name,
--   symbol,
--   amp,
--   total_weight,
--   pool_type,
--   pool_type_version,
--   total_liquidity,
--   swap_enabled,
--   swap_fee,
--   null as total_shares,
--   null as total_swap_volume,
--   token_balance,
--   token_weight,
--   token_price_rate,
--   token_address,
--   token_symbol,
--   token_index,
--   token_latest_usd_price,
--   token_latest_usd_price_timestamp,
--   _dagster_load_timestamp,
--   _dagster_partition_type,
--   _dagster_partition_key,
--   _dagster_partition_time
-- FROM `tokenlogic-data.raw_data.common_balancer_pool_liquidity_backup`;




select * from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` limit 5;
select * from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` where block_hour = (select max(block_hour) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity`) and symbol = '20wstETH-80AAVE';
select * from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE';
select distinct(block_hour) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' order by block_hour;
select distinct(symbol) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity`;
select min(block_hour) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity`;


select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` limit 5;
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where block_hour = (select max(block_hour) from `tokenlogic-data.raw_data.common_balancer_pool_liquidity`) and symbol = '20wstETH-80AAVE';
select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' order by block_hour desc limit 20;
select distinct(block_hour) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where symbol = '20wstETH-80AAVE' order by block_hour ;
select distinct(symbol) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity`;
select min(block_hour) from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity`;



select * from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` limit 100;
select distinct(id) from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` order by id;
select distinct(dynamicData_aprItems) from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results`;
select distinct(symbol) from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` order by symbol;
select * from `tokenlogic-data-dev.raw_data.common_balancer_apiv3_results` where symbol = '20wstETH-80AAVE';
select * from `tokenlogic-data.raw_data.common_balancer_apiv3_results` where symbol = '20wstETH-80AAVE';


select token_latest_usd_price from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` where token_symbol = 'AAVE' and symbol = '20wstETH-80AAVE';






with balancer_pool_liquidity_latest_per_day as (
  select * 
    , row_number() over (partition by cast(block_hour as date) order by token_symbol) as rn
  from (
    select *
      , rank() over (partition by cast(block_hour as date) order by block_hour desc) as rnk
      from `tokenlogic-data.raw_data.common_balancer_pool_liquidity` 
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
order by block_hour




select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis`;
select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` order by block_hour;
select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` order by date;

select * from `tokenlogic-data-dev.raw_data.common_balancer_pool_liquidity` where cast(block_hour as date) = '2024-02-11' and pool_address = '0x3de27efa2f1aa663ae5d458857e731c129069f29';



with tt as (
  select 
      apr.date
      , apr.cumulative_num_stk_lp_tokens_total
      , apr.tvs
      , apr.usd_emission_per_day
      -- , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total as user_pool_share -- Calculate user's initial share of the staked bpt tokens
      , inv.user_lp_tokens as balance
      , inv.user_lp_tokens + (sum(apr.usd_emission_per_day) over (order by apr.date)) / apr.tvs as compounded_balance
      , row_number() over (order by apr.date) as seqnum
    from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr
    left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
    where apr.date between '2024-02-11' and '2024-02-17'
)
, cte as (
  (select tt.*
  from tt
  where seqnum = 7)

  union all

  select 
    tt.date
    , tt.cumulative_num_stk_lp_tokens_total
    , tt.tvs
    , tt.usd_emission_per_day
    , tt.balance
    , 1+((sum(apr.usd_emission_per_day) over (order by apr.date rows between 6 preceding and current row)) / apr.tvs), tt.seqnum
  from cte join
    tt
    on tt.seqnum = cte.seqnum + 7
)
select cte.*
from cte;


WITH RECURSIVE CTE_1 AS (
    (SELECT 1 AS iteration UNION ALL SELECT 1 AS iteration)
    UNION ALL
    SELECT iteration + 1 AS iteration FROM CTE_1 WHERE iteration < 3
  )
SELECT iteration FROM CTE_1
ORDER BY 1 ASC


with recursive cte as (
  (
    select 
      apr.date as date
      , apr.cumulative_num_stk_lp_tokens_total
      , apr.tvs 
      , apr.usd_emission_per_day
      , inv.user_lp_tokens 
      , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total as user_pool_share
      , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward
      , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.cumulative_num_stk_lp_tokens_total as lp_tokens_daily_reward
    from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr
    left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
    where cast(inv.block_hour as date) = '2024-02-11' 
      and apr.date = '2024-02-11'
  )

  union all 

  select
    date_add(c.date, interval 1 day) as date
    , apr.cumulative_num_stk_lp_tokens_total
    , apr.tvs
    , apr.usd_emission_per_day
    , c.user_lp_tokens + c.lp_tokens_daily_reward as user_lp_tokens
    , (c.user_lp_tokens + c.lp_tokens_daily_reward) / apr.cumulative_num_stk_lp_tokens_total as user_pool_share
    , (c.user_lp_tokens + c.lp_tokens_daily_reward) / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day as usd_daily_reward
    , (c.user_lp_tokens + c.lp_tokens_daily_reward) / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day / apr.tvs * apr.cumulative_num_stk_lp_tokens_total as lp_tokens_daily_reward
  from cte c
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr on date_add(c.date, INTERVAL 1 day) = apr.date
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
  where c.date < '2024-02-15'
)
select * from cte
order by date


WITH RECURSIVE compounding AS (
    (
      select 
        apr.date
        , apr.cumulative_num_stk_lp_tokens_total
        , apr.tvs
        , apr.usd_emission_per_day
        , inv.user_lp_tokens as balance
        , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total as user_pool_share -- Calculate user's initial share of the staked bpt tokens
        , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day as user_daily_reward
        -- , inv.user_lp_tokens + (sum(apr.usd_emission_per_day) over (order by apr.date)) / apr.tvs as compounded_balance
      from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr
      left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
      where apr.date = '2024-02-11'
    )
    UNION ALL
    SELECT iteration + 1 AS iteration FROM CTE_1 WHERE iteration < 3
  )
SELECT iteration FROM CTE_1
ORDER BY 1 ASC


with compounding as (
  (
    select 
      apr.date
      , apr.cumulative_num_stk_lp_tokens_total
      , apr.tvs
      , apr.usd_emission_per_day
      , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total as user_pool_share -- Calculate user's initial share of the staked bpt tokens
      , inv.user_lp_tokens as balance
      , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total * apr.usd_emission_per_day as user_daily_reward
      -- , inv.user_lp_tokens + (sum(apr.usd_emission_per_day) over (order by apr.date)) / apr.tvs as compounded_balance
    from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr
    left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
    -- where apr.date between '2024-02-11' and '2024-02-17'
  )

  union all 


  select 
    apr.date
    , apr.cumulative_num_stk_lp_tokens_total
    , apr.tvs
    , apr.usd_emission_per_day
    , c.compounded_balance as balance
    , c.compounded_balance + (sum(apr.usd_emission_per_day) over (order by apr.date rows between 6 preceding and current row)) / apr.tvs as compounded_balance
  from compounding c
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr on apr.date = (c.date + interval '7' day)
)
select * from compounding


with daily_rewards as (
  select 
    apr.date
    , apr.cumulative_num_stk_lp_tokens_total
    , apr.tvs
    , inv.user_lp_tokens as no_compound_user_lp_tokens
    , inv.lp_user_total_value as no_compound_user_total_value
    , apr.usd_emission_per_day
    , inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total as initial_user_pool_share -- Calculate user's initial share of the staked bpt tokens
    -- , (inv.user_lp_tokens / apr.cumulative_num_stk_lp_tokens_total) * apr.usd_emission_per_day as user_daily_reward_usd
    , inv.aave_usd_price
    , inv.wsteth_usd_price
    , cast(floor((row_number() over (order by apr.date) - 1) / 7) as int64) as group_number
    , mod(row_number() over (order by apr.date) - 1, 7) + 1 as day_in_group
    , case 
        when mod(row_number() over (order by apr.date), 7) = 0 then true
        else false
        end as is_compounding_day
  from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr` apr
  left join `tokenlogic-data-dev.datamart_aave.aave_stkbpt_investment_analysis` inv on apr.date = cast(inv.block_hour as date)
  where apr.date > '2024-02-10'
  order by apr.date
)
, compounding1 as (
  select 
    *
    , case 
        when group_number = 0 then sum(initial_user_pool_share*usd_emission_per_day) over (partition by group_number order by day_in_group rows between unbounded preceding and current row)
        else null 
        end as compound_usd_value
  from daily_rewards
  order by date
)
, compounding2 as (
  select 
    *
    , 0.8*compound_usd_value/aave_usd_price as num_aave_tokens_compound
    , 0.8*compound_usd_value as value_aave_compound
    , 0.2*compound_usd_value/wsteth_usd_price as num_wsteth_tokens_compound
    , 0.2*compound_usd_value as value_wsteth_compound
    , case 
        when is_compounding_day = true then compound_usd_value / tvs * cumulative_num_stk_lp_tokens_total 
        else null 
        end as compound_lp_token_amount
  from compounding1
)
, compounding3 as (
  select 
    *
    , sum(ifnull(compound_lp_token_amount, 0)) over (order by date rows between unbounded preceding and current row) AS cumulative_compound_lp_tokens
    , case 
        when is_compounding_day = true then no_compound_user_lp_tokens + sum(ifnull(compound_lp_token_amount, 0)) over (order by date rows between unbounded preceding and current row)
        else null
        end as user_lp_tokens_after_compounding_tmp
  from compounding2
  order by date
)
, compounding4 as (
  select 
    *
    , case
        when group_number = 0 and day_in_group <> 7 then no_compound_user_lp_tokens 
        else last_value(user_lp_tokens_after_compounding_tmp ignore nulls) over (order by date) 
        end as compound_user_lp_tokens
  from compounding3
)
select 
  *
  , compound_user_lp_tokens / cumulative_num_stk_lp_tokens_total * tvs as compounding_user_total_value
from compounding4






select * from `tokenlogic-data-dev.events.aave_stk_token_transfer` where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_day desc limit 100; --stkAAVEwstETHBPTv2
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



-- number of staked and unstaked BPTs from the safety module per day
with deposits as (
  select 
    cast(block_day as date) as day
    , sum(value) as num_bpts_staked
  from `tokenlogic-data-dev.events.aave_stk_token_transfer`
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
  from `tokenlogic-data-dev.events.aave_stk_token_transfer`
  -- from {{ source('events', 'aave_stk_token_transfer') }}
  where `to` = '0x0000000000000000000000000000000000000000'
    and stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  group by day
  order by day
)
select 
  d.day
  , d.num_bpts_staked
  , w.num_bpts_unstaked
from deposits d
left join withdrawals w on d.day = w.day
order by d.day


select * from `tokenlogic-data-dev.events.aave_stk_token_redeem` limit 5;
select distinct(symbol) from `tokenlogic-data-dev.events.aave_stk_token_redeem`;
select * from `tokenlogic-data-dev.events.aave_stk_token_redeem` where symbol = 'stkAAVEwstETHBPTv2';
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_claimed` limit 5;
select distinct contract_address from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_claimed`;
select * from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_claimed` where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101';
select * from `tokenlogic-data-dev.raw_data.erc20_metadata` limit 5;

-- create or replace view `tokenlogic-data-dev.events.aave_stk_token_rewards_claimed` as (
--   select 
--     timestamp_seconds(r.block_timestamp) as block_timestamp
--     , date_trunc(timestamp_seconds(r.block_timestamp), hour) as block_hour
--     , date_trunc(timestamp_seconds(r.block_timestamp), day) as block_day
--     , r.block_height
--     , r.tx_index
--     , r.log_index
--     , r.tx_hash
--     , r.contract_address
--     , m.symbol
--     , r.chain
--     , r.market
--     , r.from_ as `from`
--     , r.`to`
--     , cast(r.amount_raw as bignumeric) / pow(10, 18) as amount
--   from `tokenlogic-data-dev.indexer_prod.stk_token_rewards_claimed` r 
--   left join `tokenlogic-data-dev.raw_data.erc20_metadata` m
--     on r.chain = m.chain 
--     and r.contract_address = m.token_address
-- )
;


select * 
from `tokenlogic-data-dev.events.aave_stk_token_rewards_claimed`
where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
  and `from` = '0xf23c8539069c471f5c12692a3471c9f4e8b88bc2'
order by block_timestamp
;


select * from `tokenlogic-data-dev.events.aave_stk_token_rewards_claimed`
where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101'
and `from` <> `to`



with duration_between_claims as (
  select 
    block_timestamp
    , lag(block_timestamp) over (order by block_timestamp) as previous_timestamp
  from `tokenlogic-data-dev.events.aave_stk_token_rewards_claimed`
  -- from {{ source('events', 'aave_stk_token_rewards_claimed') }}
  where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
  order by block_timestamp
)
, duration_between_claims_top_20_lps as (
  select 
    block_timestamp
    , lag(block_timestamp) over (order by block_timestamp) as previous_timestamp
  from `tokenlogic-data-dev.events.aave_stk_token_rewards_claimed`
  -- from {{ source('events', 'aave_stk_token_rewards_claimed') }}
  where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' --stkAAVEwstETHBPTv2
    and `from` in (select address from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_composition` order by perc limit 20)
    -- and `from` in (select address from {{ ref('aave_stkbpt_composition') }} order by perc limit 20)
  order by block_timestamp
)
select 
  -- Average duration for all claims
  (
    select 
      avg(timestamp_diff(block_timestamp, previous_timestamp, second)) / 3600 as avg_duration_hours --convert seconds to hours
    from duration_between_claims
    where previous_timestamp is not null --ignore the first ever claim
  ) as avg_duration

  -- Average duration for top 20 LPs claims
  , (
    select 
      avg(timestamp_diff(block_timestamp, previous_timestamp, second)) / 3600 as avg_duration_top_20_lps_hours --convert seconds to hours
    from duration_between_claims_top_20_lps
    where previous_timestamp is not null 
  ) as avg_duration_top_20_lps



select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_avg_duration_between_claims`;


select * from `tokenlogic-data-dev.datamart_aave.aave_stkbpt_daily_stakes`;


