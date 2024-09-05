{{ 
config(
    enabled=false, 
    severity="warn",
    tags=["procurement", "usage","prc_usage"]
)
}}


{% set old_etl_relation = ref("conv_wbx_prc_usage_fact") %}


{% set dbt_relation = ref("fct_wbx_prc_usage") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "item_guid",
            "business_unit_address_guid",
            "account_guid",
            "load_date",
            "update_date"
        ],
        primary_key="UNIQUE_KEY",
        summarize=false
    )
}}
