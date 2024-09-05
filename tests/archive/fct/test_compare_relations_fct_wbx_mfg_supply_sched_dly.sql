{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_inv_wtx_supply_sched_dly_fact')%}

{% set b_relation=ref('fct_wbx_mfg_supply_sched_dly') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["LOAD_DATE","UPDATE_DATE"],
    primary_key="UNIQUE_KEY",
    summarize=false
) }}
