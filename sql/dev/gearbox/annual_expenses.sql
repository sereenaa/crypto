select * from raw_data_gearbox.gb_annual_expenses where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses);

drop table if exits raw_data_gearbox.gb_annual_expenses;

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
group by discipline
order by value desc
;

select * from datamart_gearbox.gearbox_annual_expenses_by_discipline;


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

select * from datamart_gearbox.gearbox_annual_expenses_detail;



-- TABLE 3: 
select 
  initiative
  , recipient_name
  , monthly_amount_gear
  , monthly_amount_stablecoin
  , monthly_amount_usd
  , yearly_budget_gear
  , yearly_budget_stablecoin
  , yearly_budget_usd
from raw_data_gearbox.gb_contributors
-- from {{ source('raw_data_gearbox', 'gb_contributors') }}
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_contributors)
-- where table_last_updated = (select max(table_last_updated) from {{ source('raw_data_gearbox', 'gb_contributors') }})
-- order by initiative, recipient_name

union all

select
  expense_type as initiative
  , recipient_name
  , case 
      when token_symbol = 'GEAR' then token_quantity / 12
      else null
      end as monthly_amount_gear
  , stablecoin_quantity / 12 as monthly_amount_stablecoin
  , usd / 12 as monthly_amount_usd
  , case 
      when token_symbol = 'GEAR' then token_quantity
      else null
      end as yearly_budget_gear
  , stablecoin_quantity as yearly_budget_stablecoin
  , usd as yearly_budget_usd 
from raw_data_gearbox.gb_annual_expenses
-- from {{ source('raw_data_gearbox', 'gb_annual_expenses') }}
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses)
-- where table_last_updated = (select max(table_last_updated) from {{ source('raw_data_gearbox', 'gb_annual_expenses') }})

order by initiative, recipient_name



select * from raw_data_gearbox.gb_annual_expenses
where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_annual_expenses);
select * from datamart_gearbox.gearbox_annual_expenses_by_discipline;
select * from raw_data_gearbox.gb_contributors where table_last_updated = (select max(table_last_updated) from raw_data_gearbox.gb_contributors);
select * from datamart_gearbox.gearbox_annual_expenses_contributors order by initiative, recipient_name;

-- drop table raw_data_gearbox.gb_contributors;

