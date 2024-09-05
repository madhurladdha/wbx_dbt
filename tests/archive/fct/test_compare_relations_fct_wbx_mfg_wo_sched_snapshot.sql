{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation=ref('conv_mfg_wtx_wo_sched_snapsht_fact') %} 

{% set dbt_relation=ref('fct_wbx_mfg_wo_sched_snapshot') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 

    exclude_columns=[
        "wo_sched_snapsht_guid",
        "business_unit_address_guid",
        "customer_address_number_guid",
        "item_guid",
        "load_date",
        "update_date"
    ], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
