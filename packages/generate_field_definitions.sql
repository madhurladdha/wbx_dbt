/*
## modify database_name, schema_name, table_name values and compile this.
## you may also run following command to export the fields with casting
## dbt run-operation get_field_definitions --args "{'database_name': 'postsnowp', 'schema_name': 'ei_rdm', 'table_name': 'adr_customer_master_dim'}"
*/

{#- Get the field definitions that exist  -#}
{% set field_definitions = get_field_definitions(database_name = 'WBX_PROD'
, schema_name = 'FACT'
, table_name = 'fct_wbx_inv_aging'
) %}


{% for row in field_definitions %}
    {{row[1]}}  ,
{%- endfor -%} 
    cast(unique_key as text(255) ) as unique_key