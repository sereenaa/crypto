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