{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags="rdm_core"
    )
}}

/* Mike Traub 5/29/2024
    The upstream logic for this model has been changed significantly in the stage and int models.  Here are some of the details for the changes.
    The changes included in the pipeline for loading dim_wbx_uom, the primary UOM conversion table for Weetabix for the D365 project.
    These changes are needed due to adjustments in how the relevant Item UOM conversions and weights are stored and prioritized for reporting purposes.
    Changes included:
        -For CA to KG we want to ensure that the Net Weight from INVENTTABLE take priority.  In AX, the Net Weight was stored in whsphysdimuom but is not in D365.
        -For Pallets to KG we will use the Pallet to CA multiplied by the Net Weight for those items.  This too will not use the conversion stored in whsphysdimuom.
        -Includes logic to ensure that only 1 conversion (net weight is used for any item-company combination.)
*/

with int as
(
    select * from {{ ref('int_d_wbx_uom') }}
),

hist_uom as 
(
    select * from {{ ref('conv_dim_wbx_uom') }}
),

int_uom as 
(
 select
 {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_ITEM_IDENTIFIER']) }} as ITEM_GUID,
 source_item_identifier,
  source_system,
  TRIM(UPPER(from_uom)) as from_uom, 
  TRIM(UPPER(to_uom)) as to_uom, 
  conversion_rate,
  inversion_rate,
  effective_date,
  expiration_date,
  active_flag

 from INT
),

EXP_NORMALIZATION as 
(
    select 
        SRC.SOURCE_ITEM_IDENTIFIER,
        SRC.SOURCE_SYSTEM,
        SRC.ACTIVE_FLAG,
        TO_NUMBER(SRC.CONVERSION_RATE, 38, 10) as CONVERSION_RATE,
        TO_NUMBER(CASE WHEN SRC.CONVERSION_RATE>0 THEN 1/SRC.CONVERSION_RATE ELSE 1 END, 38,10) as INVERSION_RATE,
        SRC.EFFECTIVE_DATE,
        SRC.EXPIRATION_DATE,
        SRC.ITEM_GUID,
        NVL(TO_CHAR(FROM_UOM_LKP.NORMALIZED_VALUE), TO_CHAR(SRC.FROM_UOM)) AS FROM_UOM,
        NVL(TO_CHAR(TO_UOM_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.TO_UOM))      AS TO_UOM
    from INT_UOM SRC
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ITEM','PRIMARY_UOM','SRC.FROM_UOM','FROM_UOM_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ITEM','PRIMARY_UOM','SRC.TO_UOM','TO_UOM_LKP') }}
),

TARGET_TABLE AS 
(
    SELECT distinct
        {{ dbt_utils.surrogate_key(['ITEM_GUID','TO_UOM','FROM_UOM', 'EFFECTIVE_DATE', 'EXPIRATION_DATE', 'ACTIVE_FLAG']) }} as UNIQUE_KEY,
        item_guid,
        null as item_guid_old,
        source_item_identifier,
        source_system,
        from_uom,
        to_uom,
        conversion_rate,
        inversion_rate,
        effective_date,
        expiration_date,
        active_flag
    FROM EXP_NORMALIZATION
),

NEW_DIM as 
(
    select
    cast(a.item_guid as text(255) )                      as item_guid,
    cast(b.item_guid_old as text(255) )                  as item_guid_old,
    cast(a.source_item_identifier as text(255) )         as source_item_identifier,
    cast(substring(a.active_flag,1,255) as text(255) )   as active_flag,
    cast(a.effective_date as timestamp_ntz(9) )          as effective_date,
    cast(a.expiration_date as timestamp_ntz(9) )         as expiration_date,
    cast(a.conversion_rate as number(38,10) )            as conversion_rate,
    cast(a.inversion_rate as number(38,10) )             as inversion_rate,
    cast(substring(a.from_uom,1,255) as text(255) )      as from_uom,
    cast(substring(a.to_uom,1,255) as text(255) )        as to_uom,
    cast(substring(a.source_system,1,255) as text(255) ) as source_system, 
    cast(a.unique_key as text(255) )                     as unique_key 
    from TARGET_TABLE A
    LEFT JOIN HIST_UOM B ON A.UNIQUE_KEY = B.UNIQUE_KEY
),


OLD_DIM as 
(
    select
    cast(a.item_guid as text(255) )                      as item_guid,
    cast(a.item_guid_old as text(255) )                  as item_guid_old,
    cast(a.source_item_identifier as text(255) )         as source_item_identifier,
    cast(substring(a.active_flag,1,255) as text(255) )   as active_flag,
    cast(a.effective_date as timestamp_ntz(9) )          as effective_date,
    cast(a.expiration_date as timestamp_ntz(9) )         as expiration_date,
    cast(a.conversion_rate as number(38,10) )            as conversion_rate,
    cast(a.inversion_rate as number(38,10) )             as inversion_rate,
    cast(substring(a.from_uom,1,255) as text(255) )      as from_uom,
    cast(substring(a.to_uom,1,255) as text(255) )        as to_uom,
    cast(substring(a.source_system,1,255) as text(255) ) as source_system, 
    cast(a.unique_key as text(255) )                     as unique_key 
    from HIST_UOM A
    LEFT JOIN TARGET_TABLE B ON A.UNIQUE_KEY = B.UNIQUE_KEY where B.source_system is null
),

Final as
(
select * from NEW_DIM
union
select * from OLD_DIM
)



select * from Final
