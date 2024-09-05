{{ config(enabled=false, severity='warn', warn_if='>0') }}

{% set old_etl_relation = ref('conv_sls_wtx_pcos_bom_fact') %}

{% set dbt_relation = ref('fct_wbx_sls_pcos_bom') %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["LOAD_DATE","UNIQUE_KEY"],
        primary_key="UNIQUE_KEY",
        summarize=true,
    )
}}
