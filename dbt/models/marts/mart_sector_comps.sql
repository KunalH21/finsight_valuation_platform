with valuation_inputs as (
    select * from {{ ref('int_valuation_inputs') }}
),

sector_medians as (
    select 
        sector,
        period_date,
        -- Existing valuation medians (might be NULL if market cap is missing)
        percentile_cont(0.5) within group (order by ev_to_ebitda) over (partition by sector, period_date) as median_ev_ebitda,
        percentile_cont(0.5) within group (order by pe_ratio) over (partition by sector, period_date) as median_pe_ratio,
        
        -- NEW: Operational Medians (Will be POPULATED and impressive!)
        percentile_cont(0.5) within group (order by gross_margin) over (partition by sector, period_date) as median_gross_margin,
        percentile_cont(0.5) within group (order by operating_margin) over (partition by sector, period_date) as median_operating_margin,
        
        count(*) over (partition by sector, period_date) as company_count_in_sector
    from valuation_inputs
)

select distinct * from sector_medians