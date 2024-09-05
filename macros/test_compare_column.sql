{%- set columns_to_compare=adapter.get_columns_in_relation(ref('fin_account_dim'))  -%}

{{ columns_to_compare }}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}
        
        {{ column.name }}

    {% endfor %}
{% endif %}
