{{ config(
  enabled=false,
  severity = 'warn',
  warn_if = '>1'
) }}


{% set old_etl_relation %}
  select
    "WORKFLOW_PROFILE"           ,      
    "SOURCEID"           ,      
    "SOURCE_DESC"           ,      
    "TIME"           ,      
    "SCENARIO"           ,      
    "VIEW"           ,      
    "SOURCE_ENTITY"           ,      
    "TARGET_ENTITY"           ,      
    "SOURCE_ACCOUNT"           ,      
    "TARGET_ACCOUNT"           ,      
    "SOURCE_FLOW"           ,      
    "TARGET_FLOW"           ,      
    "ORIGIN"           ,      
    "SOURCE_IC"           ,      
    "TARGET_IC"           ,      
    "SOURCE_MAINUD1"           ,      
    "TARGET_MAINUD1"           ,      
    "SOURCE_MAINUD2"           ,      
    "TARGET_MAINUD2"           ,      
    "SOURCE_MAINUD3"           ,      
    "TARGET_MAINUD3"           ,      
    ROUND("AMOUNT",2) AS AMOUNT     
  from {{ source('EI_RDM','fin_onestream_stg') }}
  where workflow_profile like 'Weetabix%' and SCENARIO IN ('Actual','Budget')
{% endset %}

{% set dbt_relation %}
  select
   "WORKFLOW_PROFILE"           ,      
    "SOURCEID"           ,      
    "SOURCE_DESC"           ,      
    "TIME"           ,      
    "SCENARIO"           ,      
    "VIEW"           ,      
    "SOURCE_ENTITY"           ,      
    "TARGET_ENTITY"           ,      
    "SOURCE_ACCOUNT"           ,      
    "TARGET_ACCOUNT"           ,      
    "SOURCE_FLOW"           ,      
    "TARGET_FLOW"           ,      
    "ORIGIN"           ,      
    "SOURCE_IC"           ,      
    "TARGET_IC"           ,      
    "SOURCE_MAINUD1"           ,      
    "TARGET_MAINUD1"           ,      
    "SOURCE_MAINUD2"           ,      
    "TARGET_MAINUD2"           ,      
    "SOURCE_MAINUD3"           ,      
    "TARGET_MAINUD3"           , 
    ROUND("AMOUNT",2) AS AMOUNT     
  from {{ ref('stg_d_wbx_fin_onestream') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_etl_relation,
    b_query=dbt_relation
) }}