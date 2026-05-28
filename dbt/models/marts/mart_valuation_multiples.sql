with valuation_inputs as (
    select * from {{ ref('int_valuation_inputs') }}
),

-- Step 2: Format and Plate the data for Analysts
final as (
    select
        ticker,
        period_date,
        
        -- Core Financials (Atoms)
        revenue,
        ebitda,
        net_income,
        market_cap,
        enterprise_value,
        gross_margin,     -- ADD THIS
        operating_margin,
        
        -- Calculated Ratios (The 'Gold')
        ev_to_ebitda,
        pe_ratio,
        ev_to_revenue,
        pb_ratio,

        -- Presentation Formatting
        year(period_date) as fiscal_year,
        'Q' || quarter(period_date) as fiscal_quarter,
        
        -- The Quality Flag we built in Intermediate
        data_completeness_flag
    from valuation_inputs
)

select * from final