{{ 
config(
    enabled=false, 
    severity="warn",
    tags=["sales", "terms","sls_terms"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
)
}}


{% set old_etl_relation = ref("conv_sls_wbx_terms") %} 

{% set dbt_relation = ref("fct_wbx_sls_terms") %} 


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "item_guid",
            "customer_address_number_guid",
            "load_date",
            "update_date"
        ],
        primary_key="UNIQUE_KEY",
        summarize=true
    )
}}
