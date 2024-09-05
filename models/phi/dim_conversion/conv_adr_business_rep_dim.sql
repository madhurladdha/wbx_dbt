{{
    config(
        materialized='view'
    )
}}

with old_dim as (
    select 'BUSINESS_REP' as GENERIC_ADDRESS_TYPE,* from {{source('EI_RDM','adr_business_rep_dim')}}
    where SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' and  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),
converted_dim as (
    select
        GENERIC_ADDRESS_TYPE,
        {{ dbt_utils.surrogate_key(['SRC.SOURCE_SYSTEM','SRC.SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE']) }} AS REP_ADDRESS_NUMBER_GUID,
        CAST(REP_ADDRESS_NUMBER_GUID       AS VARCHAR2 (255)) AS REP_ADDRESS_NUMBER_GUID_OLD,
        TRIM(CAST (SOURCE_SYSTEM           AS VARCHAR2 (255))) AS SOURCE_SYSTEM,
        CAST (SOURCE_SYSTEM_ADDRESS_NUMBER AS VARCHAR2 (255)) AS SOURCE_SYSTEM_ADDRESS_NUMBER,
        CAST (REPRESENTATIVE_TYPE          AS VARCHAR2 (255)) AS REPRESENTATIVE_TYPE,
        CAST (REPRESENTATIVE_NAME          AS VARCHAR2 (255)) AS REPRESENTATIVE_NAME,
        CAST (DATE_INSERTED                AS VARCHAR2 (255)) AS DATE_INSERTED,
        CAST (DATE_UPDATED                 AS VARCHAR2 (255)) AS DATE_UPDATED,
        CAST (PROGRAM_ID                   AS VARCHAR2 (255)) AS PROGRAM_ID
    from old_dim SRC
)

select  {{ dbt_utils.surrogate_key(['REP_ADDRESS_NUMBER_GUID']) }} AS UNIQUE_KEY,* from converted_dim 