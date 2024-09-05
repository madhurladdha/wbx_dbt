-- a few cols have been excluded from the comparison test as those are null in IICS
-- world and have not been brought over
{{ config(enabled=false, severity="warn", warn_if=">0") }}

{% set old_etl_relation = ref("conv_inv_wtx_aging_fact") %}

{% set dbt_relation = ref("fct_wbx_inv_aging") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "BUSINESS_UNIT_ADDRESS_GUID","LOCATION_GUID","LOT_GUID","LOAD_DATE","UPDATE_DATE","ITEM_GUID"],
        primary_key="unique_key",
        summarize=true
    )
}}