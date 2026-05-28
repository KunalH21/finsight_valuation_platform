




Week 3:

#Handling Financial Edge Cases
    - In finance, we deal with zero-revenue companies or those with no EBITDA. Standard division by zero crashes a pipeline. Instead of writing NULLIF manually 100 times, we build a reusable "Safe Divide" tool.
     code - {% macro safe_divide(numerator, denominator, decimal_places=2) %}
    round(
        cast({{ numerator }} as double) / nullif(cast({{ denominator }} as double), 0), 
        {{ decimal_places }}
    )
    {% endmacro %}


#
(venv) kunalhirwani@Kunals-MacBook-Air dbt % dbt run --select intermediate
12:12:56  Running with dbt=1.11.7
12:12:56  Registered adapter: snowflake=1.11.3
12:12:57  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
There are 3 unused configuration paths:
- models.finsight.MARTS
- models.finsight.INTERMEDIATE
- models.finsight.STAGING
12:12:57  Found 6 models, 5 sources, 525 macros
12:12:57  
12:12:57  Concurrency: 4 threads (target='dev')
12:12:57  
12:13:01  1 of 1 START sql view model RAW.int_valuation_inputs ........................... [RUN]
12:13:02  1 of 1 ERROR creating sql view model RAW.int_valuation_inputs .................. [ERROR in 1.02s]
12:13:03  
12:13:03  Finished running 1 view model in 0 hours 0 minutes and 6.19 seconds (6.19s).
12:13:03  
12:13:03  Completed with 1 error, 0 partial successes, and 0 warnings:
12:13:03  
12:13:03  Failure in model int_valuation_inputs (models/intermediate/int_valuation_inputs.sql)
12:13:03    Database Error in model int_valuation_inputs (models/intermediate/int_valuation_inputs.sql)
  000904 (42000): SQL compilation error: error line 24 at position 8
  invalid identifier 'INC.NET_INCOME'
  compiled code at target/run/finsight/models/intermediate/int_valuation_inputs.sql
12:13:03  
12:13:03    compiled code at target/compiled/finsight/models/intermediate/int_valuation_inputs.sql
12:13:03  
12:13:03  Done. PASS=0 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=1

fix = with income_statement as (
    select * from {{ ref('stg_income_statement') }}
),
balance_sheet as (
    select * from {{ ref('stg_balance_sheet') }}
),
cash_flow as (               -- New ingredient!
    select * from {{ ref('stg_cash_flow') }}
),
market_data as (
    select * from {{ ref('stg_market_data') }}
),

joined as (
    select
        inc.ticker,
        inc.period_date,
        inc.revenue,
        inc.ebitda,
        inc.operating_income,
        cf.net_income,        -- Pulling Net Income from Cash Flow
        bal.total_assets,
        bal.total_liabilities,
        mkt.market_cap,
        mkt.closing_price
    from income_statement inc
    left join balance_sheet bal 
        on inc.ticker = bal.ticker 
        and inc.period_date = bal.period_date
    left join cash_flow cf    -- The 4th Join
        on inc.ticker = cf.ticker 
        and inc.period_date = cf.period_date
    left join market_data mkt 
        on inc.ticker = mkt.ticker 
        and inc.period_date = mkt.price_date
)

select
    *,
    -- True Cost = Market Value + What is Owed (Liabilities)
    market_cap + total_liabilities as enterprise_value,
    
    -- Score 1: Price / Final Profit
    {{ safe_divide('market_cap', 'net_income') }} as pe_ratio,
    
    -- Score 2: True Cost / Raw Operating Power
    {{ safe_divide('market_cap + total_liabilities', 'ebitda') }} as ev_to_ebitda
from joined



Warehouse Governance: Schema Naming Policy".
What to explain: "Implemented a custom generate_schema_name macro to decouple physical warehouse structure from developer environment targets. This ensured that our final 'Gold' analytical tables land in a clean MARTS schema, facilitating a simplified RBAC (Role-Based Access Control) strategy for end-users