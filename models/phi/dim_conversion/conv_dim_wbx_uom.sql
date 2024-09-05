{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

WITH uom_fac AS 
(
    SELECT * FROM {{source('WBX_PROD','dim_wbx_uom')}} WHERE  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),


converted_dim as 
(
    SELECT 
        unique_key,
        ITEM_GUID_OLD,
        ITEM_GUID,
        SOURCE_ITEM_IDENTIFIER,
        SOURCE_SYSTEM,
        FROM_UOM,
        TO_UOM,
        CONVERSION_RATE,
        INVERSION_RATE,
        EFFECTIVE_DATE,
        EXPIRATION_DATE,
        uom_fac.ACTIVE_FLAG
        FROM uom_fac
        
)

Select distinct *  from converted_dim 
