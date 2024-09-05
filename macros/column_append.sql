{%- macro column_append(column_name) -%}
(   to_number({{env_var("DBT_PREFIX_D365")}}||{{column_name}})) 
{% endmacro %}