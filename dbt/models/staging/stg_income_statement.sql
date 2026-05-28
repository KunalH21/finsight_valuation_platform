with source as (
    select * from {{ source('finsight_raw', 'raw_income_statement') }}
),
renamed as (
    select
        {{ generate_financial_sk('ticker', 'report_date') }} as income_statement_id,
        ticker::string as ticker,
        sector::string as sector,
        report_date::date as period_date,
        fiscal_quarter::int as fiscal_quarter,
        ebitda::double as ebitda,
        total_revenue::double as revenue, -- Renaming for clarity
        operating_income::double as operating_income,
        gross_profit::double as gross_profit,
        data_quality_flag::string as data_quality_flag,
        ingested_at as loaded_at
    from source
)
select * from renamed