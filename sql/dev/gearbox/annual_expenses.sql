
-- TABLE 1: aggregate by discipline, exclude incentives 
select 
  discipline
  , sum(token_quantity) as gear
  , sum(stablecoin_quantity) as stablecoins
  , sum(usd) as value
from raw_data_gearbox.gb_annual_expenses
-- from {{ source('raw_data_gearbox', 'gb_annual_expenses') }}
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses)
-- where table_last_updated = (select max(table_last_updated) from {{ source('raw_data_gearbox', 'gb_annual_expenses') }})
  and discipline <> 'Incentive'
group by discipline
order by value desc
;

-- TABLE 2: Initiative breakdown, include incentives and grants 
select 
  discipline
  , recipient_name
  , expense_type
  , token_symbol
  , token_quantity
  , stablecoin_quantity
  , usd
from raw_data_gearbox.gb_annual_expenses
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses)
order by discipline, recipient_name, expense_type
;


-- TABLE 3: 
select 
  initiative
  , recipient_name
  , monthly_amount
  , yearly_budget
from raw_data_gearbox.gb_contributors
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_contributors)
-- order by initiative, recipient_name

union all

select
  expense_type as initiative
  , recipient_name
  , usd / 12 as monthly_amount
  , usd as yearly_budget 
from raw_data_gearbox.gb_annual_expenses
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses)

order by initiative, recipient_name
;