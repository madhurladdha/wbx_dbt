{% macro compare_relations_d365(a_relation, b_relation,c_filter_field,d_filter_values, exclude_columns=[], primary_key=None, summarize=true) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

{% set column_selection %}
  {% for column_name in column_names %}  {{ adapter.quote(column_name) }}     {% if not loop.last %}      ,     {% endif %} 
  {% endfor %}
{% endset %}

{% set a_query %}
select
  {{ column_selection }}
from {{ a_relation }}
{% endset %}

{% set b_query %}
select
  {{ column_selection }}
from {{ b_relation }}
{% endset %}

{{ compare_queries_d365(a_query, b_query, c_filter_field,d_filter_values,primary_key, summarize) }}

{% endmacro %}