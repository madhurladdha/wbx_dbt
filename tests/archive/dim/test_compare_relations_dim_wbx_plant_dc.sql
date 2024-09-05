{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 


{% set a_relation=ref('conv_adr_plant_dc_master_dim')%}

{% set b_relation=ref('dim_wbx_plant_dc') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["DATE_UPDATED","DATE_INSERTED","PERSON_RESPONSIBLE","PROGRAM_ID","ENTITY_LEVEL_1","ENTITY_LEVEL_2","DEFAULT_CURRENCY_CODE","ALT_SOURCE_BUSINESS_UNIT_CODE"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}