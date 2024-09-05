/*
dbt_table and conv_table are the names of the tables, ref() will handle source and schema
select_statement is defaulted to all but can be overwritten for testing purposes
include_columns is a string of all field names to be included (EX: 'UNIQUE_KEY, PROGRAM_ID, SOURCE_SYSTEM')
    defaulted to '*'

Shortcut for making easier except tests
Add filters to the query outside of the function
*/

{% macro test_except_tables(dbt_table='', conv_table='', include_columns='*', select_statement='*') -%}
    WITH TOP_TEST AS (
        SELECT 'NEW' AS FLAG, {{ include_columns }} FROM {{ ref(dbt_table) }}
        EXCEPT
        SELECT 'NEW' AS FLAG, {{ include_columns }} FROM {{ ref(conv_table) }}
    ),
    BOTTOM_TEST AS (
        SELECT 'OLD' AS FLAG, {{ include_columns }} FROM {{ ref(conv_table) }}
        EXCEPT
        SELECT 'OLD' AS FLAG, {{ include_columns }} FROM {{ ref(dbt_table) }}
    ),
    FINAL_TEST AS (
        SELECT * FROM TOP_TEST
        UNION ALL
        SELECT * FROM BOTTOM_TEST
    )
    SELECT {{ select_statement }} FROM FINAL_TEST
{%- endmacro %}