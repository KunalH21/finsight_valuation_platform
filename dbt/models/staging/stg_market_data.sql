with source as (
    select * from {{ source('finsight_raw', 'raw_market_data') }}
),
renamed as (
    select
        {{ generate_financial_sk('ticker', 'price_date') }} as market_data_id,
        ticker::string as ticker,
        price_date::date as price_date,
        closing_price::double as closing_price,
        volume::bigint as volume,
        market_cap::double as market_cap,
        ingested_at as loaded_at
    from source
)
select * from renamed