{{
    config(
    materialized = env_var("DBT_MAT_INCREMENTAL"),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags="rdm_core"
    )
}}

/*union logic with history has been shifted to immidiate upstream model as it was required for address model in enterprise*/

with INT_PLANT as (
    select
        *
    from {{ ref('int_d_wbx_plant_dc') }}
),


FINAL_DIM as (
    select distinct
        cast(substr(UNIQUE_KEY, 1, 255) as text(255)) as UNIQUE_KEY,
        cast(substr(GENERIC_ADDRESS_TYPE, 1, 255) as text(255))
            as GENERIC_ADDRESS_TYPE,
        cast(substr(PLANTDC_ADDRESS_GUID, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID,
        cast(substr(PLANTDC_ADDRESS_GUID_OLD, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID_OLD,
        cast(substr(SOURCE_SYSTEM, 1, 255) as text(255)) as SOURCE_SYSTEM,
        cast(substr(SOURCE_BUSINESS_UNIT_CODE, 1, 255) as text(255))
            as SOURCE_BUSINESS_UNIT_CODE,
        cast(substr(BUSINESS_UNIT_NAME, 1, 255) as text(255))
            as BUSINESS_UNIT_NAME,
        cast(substr(TYPE, 1, 255) as text(255)) as TYPE,
        cast(substr(DIVISION, 1, 255) as text(255)) as DIVISION,
        cast(substr(REGION, 1, 255) as text(255)) as REGION,
        cast(substr(BRANCH_OFFICE, 1, 255) as text(255)) as BRANCH_OFFICE,
        cast(substr(BUSINESS_UNIT_LONG_DESCRIPTION, 1, 255) as text(255))
            as BUSINESS_UNIT_LONG_DESCRIPTION,
        cast(substr(DEPARTMENT_TYPE, 1, 255) as text(255)) as DEPARTMENT_TYPE,
        cast(substr(CONSOLIDATED_SHIPMENT_DC_NAME, 1, 255) as text(255))
            as CONSOLIDATED_SHIPMENT_DC_NAME,
        cast(substr(COMPANY_CODE, 1, 255) as text(255)) as COMPANY_CODE,
        cast(substr(COMPANY_NAME, 1, 255) as text(255)) as COMPANY_NAME,
        cast(substr(OPERATING_COMPANY, 1, 255) as text(255))
            as OPERATING_COMPANY,
        cast(substr(SEGMENT, 1, 255) as text(255)) as SEGMENT,
        cast(substr(ACTIVE_CC_FLAG, 1, 255) as text(255)) as ACTIVE_CC_FLAG,
        cast(substr(ETL_EXCLUDE_FLAG, 1, 255) as text(255))
            as ETL_EXCLUDE_FLAG,
        cast(substr(DATE_INSERTED, 1, 255) as text(255)) as DATE_INSERTED,
        cast(substr(DATE_UPDATED, 1, 255) as text(255)) as DATE_UPDATED
    from INT_PLANT
)



select * from FINAL_DIM