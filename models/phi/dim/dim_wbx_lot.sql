{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    tags = "rdm_core",
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}

with STG_LOT_WBX as (
    select * from {{ ref('stg_d_wbx_lot') }}
),

HISTORY_LOT_WBX as (
select * from {{ ref('conv_dim_wbx_lot') }}
),


ORGANIZE_LOT_FIELDS as (
    select
        {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_BUSINESS_UNIT_CODE','SOURCE_ITEM_IDENTIFIER','SOURCE_LOT_CODE']) }} AS LOT_GUID,
        {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_BUSINESS_UNIT_CODE','GENERIC_ADDRESS_TYPE']) }}     AS BUSINESS_UNIT_ADDRESS_GUID,
        {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_ITEM_IDENTIFIER']) }}        AS ITEM_GUID,
        CAST (DATEDIFF (DAY, TO_DATE (LOT_ONHAND_DATE), CURRENT_DATE) AS NUMBER (38,10)) AS LOT_AGE_DAYS,  --age changes each day
        NULL AS business_unit_address_guid_old,
        NULL AS item_guid_old,
        NULL AS lot_guid_old,
        source_supplier_code_primary,
        source_business_unit_code,
        source_item_identifier,
        source_lot_code,
        source_system,
        lot_desc,
        lot_status_code,
        lot_status_desc,
        quarantine_flag,
        supplier_lot_code,
        to_date (lot_expiration_date) as lot_expiration_date,
        to_date (lot_onhand_date)     as lot_onhand_date,
        lot_sellby_date,
        lot_expired_flag,
        load_date,
        update_date
    from STG_LOT_WBX
),
GEN_UNIQUE_KEY as (
    select
        {{ dbt_utils.surrogate_key(['LOT_GUID','ITEM_GUID','BUSINESS_UNIT_ADDRESS_GUID'])}} AS UNIQUE_KEY,
        business_unit_address_guid_old,
        source_supplier_code_primary,
        business_unit_address_guid,
        source_business_unit_code,
        source_item_identifier,
        lot_expiration_date,
        supplier_lot_code,
        lot_expired_flag,
        lot_status_code,
        lot_status_desc,
        quarantine_flag,
        lot_onhand_date,
        lot_sellby_date,
        source_lot_code,
        item_guid_old,
        source_system,
        lot_guid_old,
        DATEDIFF(DAY, LOT_ONHAND_DATE, TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp))) as lot_age_days,----for lot age day update logic
        item_guid,
        load_date,
        update_date,
        lot_guid,
        lot_desc
    from ORGANIZE_LOT_FIELDS
),

NEW_DIM as (
    select
        CAST (SUBSTRING (A.UNIQUE_KEY,1,255)                  AS VARCHAR2 (255)) AS UNIQUE_KEY,
        CAST (SUBSTRING (A.ITEM_GUID,1,255)                   AS VARCHAR2 (255)) AS ITEM_GUID,
        CAST (SUBSTRING (A.LOT_GUID,1,255)                    AS VARCHAR2 (255)) AS LOT_GUID,
        CAST (SUBSTRING (A.SOURCE_ITEM_IDENTIFIER,1,60)       AS VARCHAR2 (60))  AS SOURCE_ITEM_IDENTIFIER,
        CAST (SUBSTRING (A.BUSINESS_UNIT_ADDRESS_GUID,1,255)  AS VARCHAR2 (255)) AS BUSINESS_UNIT_ADDRESS_GUID,
        CAST (SUBSTRING (A.SOURCE_BUSINESS_UNIT_CODE,1,60)    AS VARCHAR2 (60))  AS SOURCE_BUSINESS_UNIT_CODE,
        CAST (B.BUSINESS_UNIT_ADDRESS_GUID_OLD                AS VARCHAR2 (255)) AS BUSINESS_UNIT_ADDRESS_GUID_OLD,
        CAST (SUBSTRING (A.LOT_DESC,1,255)                    AS VARCHAR2 (255)) AS LOT_DESC,
        CAST (A.LOT_EXPIRATION_DATE                        AS TIMESTAMP_NTZ (9)) AS LOT_EXPIRATION_DATE,
        CAST (A.LOT_ONHAND_DATE                            AS TIMESTAMP_NTZ (9)) AS LOT_ONHAND_DATE,
        CAST (A.LOT_SELLBY_DATE                            AS TIMESTAMP_NTZ (9)) AS LOT_SELLBY_DATE,
        CAST (B.ITEM_GUID_OLD                              AS VARCHAR2 (255))    AS ITEM_GUID_OLD,
        CAST (A.LOT_AGE_DAYS                               AS NUMBER (38,10))    AS LOT_AGE_DAYS,
        CAST (B.LOT_GUID_OLD                               AS VARCHAR2 (255))     AS LOT_GUID_OLD,
        CAST (SUBSTRING (A.SOURCE_SUPPLIER_CODE_PRIMARY,1,60) AS VARCHAR2 (60))  AS SOURCE_SUPPLIER_CODE_PRIMARY,
        CAST (SUBSTRING (A.SUPPLIER_LOT_CODE,1,60)            AS VARCHAR2 (60))  AS SUPPLIER_LOT_CODE,
        CAST (SUBSTRING (A.LOT_EXPIRED_FLAG,1,1)              AS VARCHAR2 (1))   AS LOT_EXPIRED_FLAG,
        CAST (SUBSTRING (A.LOT_STATUS_DESC,1,255)             AS VARCHAR2 (255)) AS LOT_STATUS_DESC,
        CAST (SUBSTRING (A.LOT_STATUS_CODE,1,60)              AS VARCHAR2 (60))  AS LOT_STATUS_CODE,
        CAST (SUBSTRING (A.SOURCE_LOT_CODE,1,60)              AS VARCHAR2 (60))  AS SOURCE_LOT_CODE,
        CAST (SUBSTRING (A.QUARANTINE_FLAG,1,1)               AS VARCHAR2 (1))   AS QUARANTINE_FLAG,
        CAST (SUBSTRING (A.SOURCE_SYSTEM,1,30)                AS VARCHAR2 (30))  AS SOURCE_SYSTEM,
        CAST (A.UPDATE_DATE                                   AS TIMESTAMP_NTZ (9)) AS UPDATE_DATE,
        CAST (A.LOAD_DATE                                     AS TIMESTAMP_NTZ (9)) AS LOAD_DATE
from GEN_UNIQUE_KEY A
LEFT JOIN HISTORY_LOT_WBX B ON A.UNIQUE_KEY = B.UNIQUE_KEY
),

OLD_DIM as (
    select
        CAST (SUBSTRING (A.UNIQUE_KEY,1,255)                  AS VARCHAR2 (255)) AS UNIQUE_KEY,
        CAST (SUBSTRING (A.ITEM_GUID,1,255)                   AS VARCHAR2 (255)) AS ITEM_GUID,
        CAST (SUBSTRING (A.LOT_GUID,1,255)                    AS VARCHAR2 (255)) AS LOT_GUID,
        CAST (SUBSTRING (A.SOURCE_ITEM_IDENTIFIER,1,60)       AS VARCHAR2 (60))  AS SOURCE_ITEM_IDENTIFIER,
        CAST (SUBSTRING (A.BUSINESS_UNIT_ADDRESS_GUID,1,255)  AS VARCHAR2 (255)) AS BUSINESS_UNIT_ADDRESS_GUID,
        CAST (SUBSTRING (A.SOURCE_BUSINESS_UNIT_CODE_NEW,1,60)    AS VARCHAR2 (60))  AS SOURCE_BUSINESS_UNIT_CODE,
        CAST (A.BUSINESS_UNIT_ADDRESS_GUID_OLD                AS VARCHAR2 (255)) AS BUSINESS_UNIT_ADDRESS_GUID_OLD,
        CAST (SUBSTRING (A.LOT_DESC,1,255)                    AS VARCHAR2 (255)) AS LOT_DESC,
        CAST (A.LOT_EXPIRATION_DATE                        AS TIMESTAMP_NTZ (9)) AS LOT_EXPIRATION_DATE,
        CAST (A.LOT_ONHAND_DATE                            AS TIMESTAMP_NTZ (9)) AS LOT_ONHAND_DATE,
        CAST (A.LOT_SELLBY_DATE                            AS TIMESTAMP_NTZ (9)) AS LOT_SELLBY_DATE,
        CAST (A.ITEM_GUID_OLD                              AS VARCHAR2 (255))    AS ITEM_GUID_OLD,
        CAST (A.LOT_AGE_DAYS                               AS NUMBER (38,10))    AS LOT_AGE_DAYS,
        CAST (A.LOT_GUID_OLD                               AS VARCHAR2 (255))    AS LOT_GUID_OLD,
        CAST (SUBSTRING (A.SOURCE_SUPPLIER_CODE_PRIMARY,1,60) AS VARCHAR2 (60))  AS SOURCE_SUPPLIER_CODE_PRIMARY,
        CAST (SUBSTRING (A.SUPPLIER_LOT_CODE,1,60)            AS VARCHAR2 (60))  AS SUPPLIER_LOT_CODE,
        CAST (SUBSTRING (A.LOT_EXPIRED_FLAG,1,1)              AS VARCHAR2 (1))   AS LOT_EXPIRED_FLAG,
        CAST (SUBSTRING (A.LOT_STATUS_DESC,1,255)             AS VARCHAR2 (255)) AS LOT_STATUS_DESC,
        CAST (SUBSTRING (A.LOT_STATUS_CODE,1,60)              AS VARCHAR2 (60))  AS LOT_STATUS_CODE,
        CAST (SUBSTRING (A.SOURCE_LOT_CODE,1,60)              AS VARCHAR2 (60))  AS SOURCE_LOT_CODE,
        CAST (SUBSTRING (A.QUARANTINE_FLAG,1,1)               AS VARCHAR2 (1))   AS QUARANTINE_FLAG,
        CAST (SUBSTRING (A.SOURCE_SYSTEM,1,30)                AS VARCHAR2 (30))  AS SOURCE_SYSTEM, 
        CAST (A.UPDATE_DATE                                   AS TIMESTAMP_NTZ (9)) AS UPDATE_DATE,
        CAST (A.LOAD_DATE                                     AS TIMESTAMP_NTZ (9)) AS LOAD_DATE

from HISTORY_LOT_WBX A
LEFT JOIN GEN_UNIQUE_KEY B ON A.UNIQUE_KEY = B.UNIQUE_KEY WHERE B.business_unit_address_guid IS NULL
),


Final as (
    select distinct * from NEW_DIM
    UNION 
    SELECT distinct * FROM OLD_DIM
)


SELECT  * FROM FINAL
