select * from raw_data.ethereum_token_transfers limit 5;

select * from datamart_aave.aave_stkbpt_avg_duration_between_claims limit 5;


select count(*) from raw_data_aave.aave_ethereum_token_transfers;
select count(*) from raw_data_aave.aave_ethereum_token_transfers_backfill;

select * from raw_data_aave.aave_annual_expenses;
select * from datamart_aave.aave_annual_expenses;

select * from raw_data_aave.aave_manual_dao_spends order by grist_last_updated desc;
select * from raw_data_aave.aave_payment_wallet_labels order by grist_last_updated desc;
select * from raw_data_aave.aave_streams_approvals_state;
select * from datamart_aave.aave_payment_labels;
select * from datamart_aave.aave_streams_staged;
select * from datamart_aave.aave_current_streams;


-- alter table raw_data_aave.aave_manual_dao_spends add column discipline string;
-- alter table raw_data_aave.aave_payment_wallet_labels add column discipline string;


select count(*) from events.aave_pool_configurator_aave_reserve_initialized limit 5;
select * from raw_data.common_chainhour_block_numbers order by block_hour desc limit 100;
select * from raw_data_aave.chainhour_protocol_data order by _dagster_load_timestamp desc limit 50;


select * from tokenlogic-data-dev.datamart_aave.aave_stkbpt_balancer_pool_deposits_withdrawals;



select * from tokenlogic-data-dev.external_tables.balancer_swap_fees_historical limit 100;


SELECT * FROM tokenlogic-data-dev.datamart_aave.aave_stkbpt_apr ORDER BY date DESC limit 30;





select * from tokenlogic-data-dev.datamart_gho.gho_ethereum_mints_burns_day order by block_day;
select * from tokenlogic-data-dev.raw_data.gho_chainhour_gho_mints limit 5;
select * from tokenlogic-data-dev.raw_data.gho_chainhour_gho_mints where cast(block_hour as date) = '2024-09-18';
select block_hour, chain, block_height, `hash`, `from`, `to`, token, symbol, amount, block_time, count(*) from tokenlogic-data-dev.raw_data.gho_chainhour_gho_mints 
group by 1,2,3,4,5,6,7,8,9,10 having count(*) > 1;