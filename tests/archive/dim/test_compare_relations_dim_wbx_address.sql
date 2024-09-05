{{ config(
  enabled=false,
  severity = 'warn'

) }}



{% set old_etl_relation = ref('conv_adr_address_master_dim') %}

{% set dbt_relation = ref('dim_wbx_address') %}



{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["DATE_INSERTED","DATE_UPDATED","CONTACT_2_EMAIL","PROGRAM_ID","ACTIVE_INDICATOR","company_code","company_code_guid","ADDRESS_GUID_OLD"],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}