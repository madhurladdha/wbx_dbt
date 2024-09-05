{{ config(enabled=false, severity="warn") }}

{% set a_relation = ref("conv_fct_wbx_sls_forecast_override") %}

{% set b_relation = ref("fct_wbx_sls_forecast_override") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=[
            "item_guid",
            "plan_customer_addr_number_guid",
            "scenario_guid"
        ],
        summarize=false,
    )
}}
