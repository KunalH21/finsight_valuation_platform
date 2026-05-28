with income_statement as (
    select * from {{ ref('stg_income_statement') }}
),
balance_sheet as (
    select * from {{ ref('stg_balance_sheet') }}
),
cash_flow as (
    select * from {{ ref('stg_cash_flow') }}
),
market_data as (
    select * from {{ ref('stg_market_data') }}
),

-- 2. Create a 'Cross-Join' of financials to all possible past market dates
-- We filter this down in the next step to keep it performant
joined_raw as (
    select 
        inc.ticker, 
        inc.period_date, 
        inc.revenue,
        inc.sector, 
        inc.ebitda, 
        inc.operating_income, 
        inc.gross_profit,
        cf.net_income,
        bal.total_assets,
        bal.total_liabilities,
        bal.total_equity, -- Our fix from the previous step
        mkt.market_cap,
        mkt.closing_price,
        -- Rank market dates: 1 is the closest date to the report
        row_number() over (
            partition by inc.ticker, inc.period_date 
            order by mkt.price_date desc
        ) as price_recency_rank
    from income_statement inc
    left join balance_sheet bal 
        on inc.ticker = bal.ticker and inc.period_date = bal.period_date
    left join cash_flow cf 
        on inc.ticker = cf.ticker and inc.period_date = cf.period_date
    -- The As-Of Join logic: Join to all market dates on or before report
    left join market_data mkt 
        on inc.ticker = mkt.ticker 
        and mkt.price_date <= inc.period_date 
)

-- 3. Filter for the #1 ranked market price and do the math
select 
    ticker,
    period_date,
    revenue,
    sector,
    ebitda,
    operating_income,
    gross_profit,
    net_income,
    total_assets,
    total_liabilities,
    total_equity,
    market_cap,
    closing_price,

    case 
        when market_cap is not null then 'FULL'
        else 'NO_MARKET_DATA'
    end as data_completeness_flag,
    -- Financial Multiples Logic
    market_cap + total_liabilities as enterprise_value,
    {{ safe_divide('enterprise_value', 'ebitda') }} as ev_to_ebitda,
    {{ safe_divide('market_cap', 'net_income') }} as pe_ratio,
    {{ safe_divide('market_cap', 'total_equity') }} as pb_ratio,
    {{ safe_divide('gross_profit', 'revenue') }} as gross_margin,
    {{ safe_divide('operating_income', 'revenue') }} as operating_margin,
    {{ safe_divide('enterprise_value', 'revenue') }} as ev_to_revenue
from joined_raw
where price_recency_rank = 1 -- Only keep the market cap closest to the report date