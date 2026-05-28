{% macro generate_financial_sk(ticker, period_date) %}
    md5(cast(coalesce(cast({{ ticker }} as string), '') || '-' || coalesce(cast({{ period_date }} as string), '') as string))
{% endmacro %}