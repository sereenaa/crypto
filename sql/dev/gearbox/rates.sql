
select * from datamart_aave.aave_deposit_rate_compare_hour limit 100;
select * from datamart_gho.gho_borrow_rate_compare_hour limit 100;

select * from raw_data_gearbox.gb_pool_data_v3 limit 100;
select distinct chain from raw_data_gearbox.gb_pool_data_v3; -- arbitrum, ethereum, optimism
select * from raw_data_gearbox.gb_pool_data_v3 where chain = 'optimism' and underlying_token_symbol = 'USDC' limit 100;
select distinct chain, dtoken_symbol, underlying_token_symbol from raw_data_gearbox.gb_pool_data_v3 order by chain, dtoken_symbol;


select 
  block_hour
  , chain
  , "Gearbox v3" as market
  , underlying_token_symbol as symbol
  , base_interest_rate as borrow_rate
  , avg(base_interest_rate) over (partition by chain, dtoken_symbol order by block_hour rows between 6 preceding and 6 following) as borrow_rate_smooth
  , supply_rate as deposit_rate
  , avg(supply_rate) over (partition by chain, dtoken_symbol order by block_hour rows between 6 preceding and 6 following) as deposit_rate_smooth
from raw_data_gearbox.gb_pool_data_v3
where base_interest_rate <> 0 or supply_rate <> 0
order by block_hour desc, chain, symbol 
limit 100;

select * from datamart_gearbox.gearbox_deposit_borrow_rate_hour;