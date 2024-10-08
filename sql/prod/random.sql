select * from `tokenlogic-data.datamart_aave.aave_stkbpt_daily_stakes`;

select * from raw_data_aave.aave_annual_expenses;
select * from datamart_aave.aave_annual_expenses;

-- alter table raw_data_aave.aave_manual_dao_spends add column discipline string;
-- alter table raw_data_aave.aave_payment_wallet_labels add column discipline string;
select * from datamart_aave.aave_current_streams;

select count(*) from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged order by block_timestamp;
select count(*) from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;
select * from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill order by block_timestamp;

-- insert into raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged
-- select 
--   block_hash, block_number, block_timestamp, transaction_hash, transaction_index, 
--   log_index, address, data, topics, removed, token_1, token_2, 
--   delta_1, delta_2, protocolFeeAmount_1, protocolFeeAmount_2, _dagster_load_timestamp, 
--   null as _dagster_partition_type, null as _dagster_partition_key, null as _dagster_partition_time
-- from raw_data_aave.aave_20wstETH_80AAVE_PoolBalanceChanged_backfill;