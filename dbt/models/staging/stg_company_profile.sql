with source as (
    select * from {{ source('finsight_raw', 'raw_company_profile') }}
),
renamed as (
    select
        md5(cast(coalesce(cast(ticker as string), '') as string)) as company_id, -- This doesn't need a date-based surrogate key because company metadata is static
        ticker::string as ticker,
        company_name::string as company_name,
        sector::string as sector,
        industry::string as industry,
        exchange::string as exchange,
        country::string as country,
        employees::int as employee_count,
        ingested_at as loaded_at
    from source
)
select * from renamed