{{
    config(
        pre_hook=[                
            "{{ truncate_if_exists(env_var('DBT_SRC_RAW_DATA_SCHEMA'), 'src_fin_onestream_stg') }}",
            "{{ ent_dbt_package.copy_into(ref('src_fin_onestream_stg'),env_var('DBT_SRC_ENT_DB'),env_var('DBT_SRC_RAW_DATA_SCHEMA')
                ,'PHI_DSCI_ONESTREAM_INTERNAL_STAGE','.*/Weetabix.*.csv','ONESTREAM_BLOB_FILE_FORMAT','1','ABORT_STATEMENT',True) }}"
            ]     
    )
}}
with fin_onestream_stg as 
(
    select * from {{ ref('src_fin_onestream_stg') }} 
),


FOS as (
SELECT 
      '{{env_var("DBT_SOURCE_SYSTEM")}}' AS source_system,
       WORKFLOW_PROFILE,
       SOURCE_IC,
       TARGET_IC,
       SOURCE_ACCOUNT,
       TARGET_ACCOUNT,
       SCENARIO,
       TIME,
       case when ltrim(rtrim(SOURCE_MAINUD1)) is null or ltrim(rtrim(SOURCE_MAINUD1)) = '' then '%' else SOURCE_MAINUD1 end as SOURCE_MAINUD1,
       TARGET_MAINUD1,
       SOURCE_MAINUD2,
       TARGET_MAINUD2,
       SOURCE_MAINUD3,
       TARGET_MAINUD3,
       SOURCEID,
       SOURCE_DESC,
       VIEW,
       SOURCE_ENTITY,
       TARGET_ENTITY,
       SOURCE_FLOW,
       TARGET_FLOW,
       ORIGIN,
       sum(amount) as amount,
       to_char(current_date(),'mm/dd/yyyy HH:MI:SS.FF6') as LOAD_DATE,
       null as FILE_NAME
       FROM fin_onestream_stg
       WHERE  workflow_profile like 'Weetabix%' and SCENARIO IN ('Actual','Budget')
       GROUP BY 
       WORKFLOW_PROFILE,
       SOURCE_IC,
       TARGET_IC,
       SOURCE_ACCOUNT,
       TARGET_ACCOUNT,
       SCENARIO,
       TIME,
       SOURCE_MAINUD1,
       TARGET_MAINUD1,
       SOURCE_MAINUD2,
       TARGET_MAINUD2,
       SOURCE_MAINUD3,
       TARGET_MAINUD3,
       SOURCEID,
       SOURCE_DESC,
       VIEW,
       SOURCE_ENTITY,
       TARGET_ENTITY,
       SOURCE_FLOW,
       TARGET_FLOW,
       ORIGIN,
       LOAD_DATE,
       FILE_NAME
      
)

select * from FOS
