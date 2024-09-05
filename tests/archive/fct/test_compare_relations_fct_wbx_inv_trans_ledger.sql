{{ config( 

  enabled=false, 
  snowflake_warehouse= "EI_MEDIUM",
  severity = 'warn', 
  warn_if = '>0' 

) }} 

/* This test is set to disabled as it does not really function as desired for this data set.  The complexities within the data and the unique key make it invalid.
    The team have written some other tests to ensure an aggregated level of validity between IICS and DBT.
*/


{% set a_relation=ref('conv_inv_wtx_trans_ledger_fact')%}

{% set b_relation=ref('fct_wbx_inv_trans_ledger') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["update_date","load_date","business_unit_address_guid","lot_guid","location_guid","item_guid","address_guid"],
    primary_key=UNIQUE_KEY,
    summarize=false
) }}