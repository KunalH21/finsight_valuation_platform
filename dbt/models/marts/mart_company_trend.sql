with valuation_inputs as (
    select * from {{ ref('int_valuation_inputs') }}
),

trends as (
    select
        ticker,
        period_date,
        sector,
        revenue,
        ebitda,
        gross_margin,
        operating_margin,
        ev_to_ebitda,
        -- LAG reaches back to grab the revenue from 1 row ago (the previous quarter)
        lag(revenue, 4) over (partition by ticker order by period_date) as prev_year_revenue,
        lag(ebitda, 4) over (partition by ticker order by period_date) as prev_year_ebitda,
        lag(gross_margin, 4) over (partition by ticker order by period_date) as prev_year_gross_margin,
        lag(ev_to_ebitda) over (partition by ticker order by period_date) as prev_quarter_ev_ebitda
    from valuation_inputs
)

select
    *,
    gross_margin - prev_year_gross_margin as margin_expansion_yoy,

    -- Use safe_divide to calculate Growth Rates without crashing on zero values
    {{ safe_divide('revenue - prev_year_revenue', 'prev_year_revenue') }} as revenue_growth_yoY,
    {{ safe_divide('ebitda - prev_year_ebitda', 'prev_year_ebitda') }} as ebitda_growth_yoY,

from trends