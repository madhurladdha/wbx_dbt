with ID as (
    select * from {{ ref('src_inventdim')}}
),

WL as (
    select * from {{ ref('src_wmslocation')}}
),

stg as (
    select DISTINCT
    '{{env_var("DBT_SOURCE_SYSTEM")}}'                   AS SOURCE_SYSTEM,
    NVL(NULLIF(UPPER(TRIM(ID.WMSLOCATIONID)),''),'-')    AS SOURCE_LOCATION_CODE,
    NVL(NULLIF(UPPER(TRIM(ID.INVENTLOCATIONID)),''),'-') AS SOURCE_BUSINESS_UNIT_CODE,
    NVL(NULLIF(UPPER(TRIM(WL.AISLEID)),''),'-')          AS SOURCE_AISLE_CODE,
    NVL(NULLIF(UPPER(TRIM(WL.POSITION)),''),'-')         AS SOURCE_BIN_CODE,
    'N'                                                  AS STAGING_LOCATION_FLAG
    FROM 	  ID
    LEFT JOIN WL ON     UPPER(TRIM (ID.WMSLOCATIONID)) =UPPER (TRIM (WL.WMSLOCATIONID)) AND UPPER (TRIM (ID.INVENTLOCATIONID)) =UPPER (TRIM (WL.INVENTLOCATIONID))
     WHERE  UPPER (TRIM (ID.WMSLOCATIONID)) IS NOT NULL AND UPPER (TRIM (ID.INVENTLOCATIONID)) IS NOT NULL

)

select * from stg