{% macro print_audit_output_all_columns() %}

{%- set columns_to_compare=adapter.get_columns_in_relation(ref('adr_customer_master_dim'))  -%}

{%- set exclude_columns=["CUSTOMER_ADDRESS_NUMBER_GUID","CUSTOMER_ADDRESS_NUMBER_GUID_OLD","COMPANY_ADDRESS_GUID","DATE_INSERTED","DATE_UPDATED"] -%}

{% set old_etl_relation_query %}
    select * from {{ ref('conv_adr_customer_master_dim') }}
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('adr_customer_master_dim') }}
{% endset %}

{% if execute %}
    
        {% for column in columns_to_compare %}
            {{ log('Comparing column "' ~ column.name ~'"', info=True) }}

            {% if column.name not in exclude_columns %}
                {% set audit_query = audit_helper.compare_column_values(
                    a_query=old_etl_relation_query,
                    b_query=new_etl_relation_query,
                    primary_key="CUSTOMER_ADDRESS_NUMBER_GUID",
                    column_to_compare=column.name
                ) %}
                
                {% set audit_results = run_query(audit_query) %}

                {% do log(audit_results.column_names, info=True) %}
                    {% for row in audit_results.rows %}
                        {% do log(row.values(), info=True) %}
                    {% endfor %}

            {% endif %}
        {% endfor %}    
{% endif %}
{% endmacro %}