{%- macro check_ax_hist_exists(schema_name, table_name) -%}

{% set ax_hist_exists_query %}
SELECT EXISTS( SELECT * FROM information_schema.tables 
WHERE lower(table_name) = lower('{{ table_name }}')
AND lower(table_schema) = lower('{{ schema_name }}')
) as table_exists
order by 1
{% endset %}

{% set results = run_query(ax_hist_exists_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{%- set results_list_out = results_list[0] | string() -%}

{% if results_list_out | string() == 'True' %}

{% set ax_query %}
select distinct source_legacy from {{ schema_name }}.{{ table_name }} where source_legacy = 'AX'
{% endset %}

{% set results_ax = run_query(ax_query) %}

{% if results_ax and results_ax.rows|length > 0 %}
{% set result_value = results_ax.rows[0][0] %}
{% if result_value == 'AX' %}
        {{ return('True') }}
{% else %}
        {{ return('False') }}
{% endif %}
{% else %}
        {{ return('False') }}
{% endif %}

{% else %}
{# Table do not exists #}

{% endif %}

{% endmacro %}