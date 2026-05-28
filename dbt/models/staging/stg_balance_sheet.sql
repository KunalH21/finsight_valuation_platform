with source as (
    select * from {{ source('finsight_raw', 'raw_balance_sheet') }}
),
renamed as (
    select
        {{ generate_financial_sk('ticker', 'report_date') }} as balance_sheet_id,
        ticker::string as ticker,
        report_date::date as period_date,
        fiscal_quarter::int as fiscal_quarter,
        total_assets::double as total_assets,
        total_equity::double as total_equity,
        total_liabilities::double as total_liabilities,
        common_stock::double as common_stock,
        retained_earnings::double as retained_earnings,
        data_quality_flag::string as data_quality_flag,
        ingested_at as loaded_at
    from source
)
select * from renamed