select * from raw_data.ethereum_token_transfers limit 5;

select * from datamart_aave.aave_stkbpt_avg_duration_between_claims limit 5;


select count(*) from raw_data_aave.aave_ethereum_token_transfers;
select count(*) from raw_data_aave.aave_ethereum_token_transfers_backfill;