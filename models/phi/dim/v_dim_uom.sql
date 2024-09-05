{{ config(
    materialized="view",
    tags="rdm_core"
) }}

/*this view is created in order to supplement the UOM Model with certain scenarios(conversion rates) that are not already present in UOM,
basically all the below cases:
WHEN FROM_UOM= TO_UOM
        or (FROM_UOM= 'LB' and TO_UOM = 'KG')
        or (FROM_UOM= 'KG' and TO_UOM = 'LB')
        or (FROM_UOM= 'CW' and TO_UOM = 'LB')
        or (FROM_UOM= 'LB' and TO_UOM = 'CW')*/

with
    wbx_uom as (select * from {{ ref("dim_wbx_uom") }}),
    dim_uom as (
        select item_guid, from_uom, to_uom, conversion_rate, inversion_rate
        from ({{ ref("dim_wbx_uom_recursive") }})
    ),

    same_uom as (
        select distinct
            'dummy' as item_guid,
            from_uom,
            from_uom as to_uom,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
        union
        select distinct
            'dummy' as item_guid,
            to_uom as from_uom,
            to_uom,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
    ),

    lb_kg_uom as (
        select distinct
            'dummy' as item_guid,
            'LB' as from_uom,
            'KG' as to_uom,
            {{ lkp_constants("LB_KG_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("KG_LB_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
        union
        select distinct
            'dummy' as item_guid,
            'KG' as from_uom,
            'LB' as to_uom,
            {{ lkp_constants("KG_LB_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("LB_KG_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
    ),

    lb_cw_uom as (
        select distinct
            'dummy' as item_guid,
            'CW' as from_uom,
            'LB' as to_uom,
            {{ lkp_constants("CW_LB_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("LB_CW_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
        union
        select distinct
            'dummy' as item_guid,
            'LB' as from_uom,
            'CW' as to_uom,
            {{ lkp_constants("LB_CW_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("CW_LB_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
    ) ,

    cw_kg_uom as (
        select distinct
            'dummy' as item_guid,
            'CW' as from_uom,
            'KG' as to_uom,
            {{ lkp_constants("CW_KG_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("KG_CW_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
        union
        select distinct
            'dummy' as item_guid,
            'KG' as from_uom,
            'CW' as to_uom,
            {{ lkp_constants("KG_CW_CONVERSION_RATE") }} as conversion_rate,
            {{ lkp_constants("CW_KG_CONVERSION_RATE") }} as inversion_rate
        from wbx_uom
    ),

    final as (
        select *
        from dim_uom
        union
        select *
        from same_uom
        union
        select *
        from lb_kg_uom
        union
        select *
        from lb_cw_uom
        union
        select *
        from cw_kg_uom
    )

select *
from final
