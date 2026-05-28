with source as (
    select * from {{ source('finsight_raw', 'raw_cash_flow') }}
),
renamed as (
    select
        {{ generate_financial_sk('ticker', 'report_date') }} as cash_flow_id,
        ticker::string as ticker,
        report_date::date as period_date,
        fiscal_quarter::int as fiscal_quarter,
        net_income::double as net_income,
        depreciation_amortization::double as depreciation_and_amortization,
        stock_based_compensation::double as stock_based_compensation,
        change_in_working_capital::double as change_in_working_capital,
        data_quality_flag::string as data_quality_flag,
        ingested_at as loaded_at
    from source
)
select * from renamed