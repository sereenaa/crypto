select * from `tokenlogic-data.datamart_aave.aave_stkbpt_daily_stakes`;

select * from raw_data_aave.aave_annual_expenses;
select * from raw_data_aave.aave_payment_wallet_labels order by grist_last_updated desc;
select * from datamart_aave.aave_annual_expenses;
select * from datamart_aave.aave_payment_labels;

-- alter table raw_data_aave.aave_manual_dao_spends add column discipline string;
-- alter table raw_data_aave.aave_payment_wallet_labels add column discipline string;
select * from datamart_aave.aave_current_streams;

select count(*) from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged order by block_timestamp desc;
select count(*) from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill order by block_timestamp desc;

-- insert into raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
-- select 
--   block_hash, block_number, block_timestamp, transaction_hash, transaction_index, 
--   log_index, address, data, topics, removed, token_1, token_2, 
--   delta_1, delta_2, protocolFeeAmount_1, protocolFeeAmount_2, _dagster_load_timestamp, 
--   null as _dagster_partition_type, null as _dagster_partition_key, null as _dagster_partition_time
-- from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;

select * from raw_data.common_chainhour_block_numbers order by block_height desc limit 100;

select * from datamart_aave.aave_stkbpt_balancer_pool_deposits_withdrawals order by day desc;


select * from prices.redstone_prices limit 10;
select distinct symbol from prices.redstone_prices order by symbol;

select * from indexer_prod.stk_token_transfer where contract_address = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_timestamp desc;
select * from events.aave_stk_token_transfer where stake_token = '0x9eda81c21c273a82be9bbc19b6a6182212068101' order by block_timestamp desc limit 100;
