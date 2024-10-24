
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
  , sum(token_quantity) as gear
  , sum(stablecoin_quantity) as stablecoins
  , sum(usd) as value
from raw_data_aave.aave_annual_expenses
-- from {{ source('raw_data_aave', 'aave_annual_expenses') }}
where table_last_updated = (select max(table_last_updated) from raw_data_aave.aave_annual_expenses)
-- where table_last_updated = (select max(table_last_updated) from {{ source('raw_data_aave', 'aave_annual_expenses') }})
group by discipline
order by value desc
;

select * from datamart_aave.aave_annual_expenses_by_discipline;