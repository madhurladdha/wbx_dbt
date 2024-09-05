{{ 
config(
    enabled=false, 
    severity="warn",
    tags = ["sls","sales","forecast","sls_forecast","sls_finance"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
)
}}


{% set old_etl_relation = ref("conv_wtx_sls_forecast") %}


{% set dbt_relation = ref("fct_wbx_sls_forecast") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "scenario_guid",
            "plan_customer_addr_number_guid"
        ],
        summarize=true
    )
}}
