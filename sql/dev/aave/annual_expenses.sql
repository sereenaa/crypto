
select 
  discipline
  , recipient_name
  , expense_type
  , token_symbol
  , token_quantity
  , stablecoin_quantity
  , usd
  , governance_link
from raw_data_aave.aave_annual_expenses
-- from {{ source('raw_data_aave', 'aave_annual_expenses') }}
where table_last_updated = (select max(table_last_updated) from raw_data_aave.aave_annual_expenses)
-- where table_last_updated = (select max(table_last_updated) from {{ source('raw_data_aave', 'aave_annual_expenses') }})
;


select 
  discipline
  , token_symbol 
  , sum(token_quantity) as token_quantity
  , sum(stablecoin_quantity) as stablecoin_quantity
  , sum(usd) as usd
from datamart_aave.aave_annual_expenses
group by discipline, token_symbol
order by usd desc
;