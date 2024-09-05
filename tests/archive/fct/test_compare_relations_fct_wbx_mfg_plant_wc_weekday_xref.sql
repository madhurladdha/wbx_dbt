
{{ config(enabled=false, severity="warn", warn_if=">0") }}

{% set old_etl_relation = ref("conv_fct_wbx_mfg_plant_wc_weekday_xref") %}

{% set dbt_relation = ref("fct_wbx_mfg_plant_wc_weekday_xref") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "LOAD_DATE","UPDATE_DATE","VERSION_NUMBER"],
        summarize=true
    )
}}