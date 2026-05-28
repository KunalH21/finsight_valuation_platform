{% macro safe_divide(numerator, denominator, decimal_places=2) %}
    round(
        cast({{ numerator }} as double) / nullif(cast({{ denominator }} as double), 0), 
        {{ decimal_places }}
    )
{% endmacro %}