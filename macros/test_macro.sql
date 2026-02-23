{% macro calculate_market_cap(high_price, low_price) %}
    ({{ high_price }} * {{ low_price }})
{% endmacro %}