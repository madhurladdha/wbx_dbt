/*this model is to calculate the UOM conversion rate using recursive CTE from UOM dim
table to handle scenarios where direct conversion rate does not exist*/
{{ config(tags="rdm_core") }}
with
    wbx_uom as (select * from {{ ref("dim_wbx_uom") }}),
    uom as (
        select
            to_char(item_guid) as item_guid,
            1 as hierarchy,
            from_uom,
            to_uom,
            conversion_rate as conversion_rate,
            inversion_rate as inversion_rate
        from wbx_uom
        -- where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        union
        select
            to_char(item_guid) as item_guid,
            1 as hierarchy,
            to_uom,
            from_uom,
            inversion_rate as conversion_rate,
            conversion_rate as inversion_rate
        from wbx_uom
        where
            -- source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}' and
            (item_guid, to_uom, from_uom) not in (
                select item_guid, from_uom, to_uom from wbx_uom
            -- where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
            )
    ),
    cte_uom as (
        select
            item_guid,
            from_uom,
            to_uom,
            (conversion_rate) as conversion_rate,
            (inversion_rate) as inversion_rate,
            hierarchy
        from uom
        where hierarchy = 1
        union all
        select
            cte_uom.item_guid item_guid,
            cte_uom.from_uom from_uom,
            src.to_uom to_uom,
            round(
                (src.conversion_rate) * (cte_uom.conversion_rate), 4
            ) as conversion_rate_cte,
            round(
                (src.inversion_rate) * (cte_uom.inversion_rate), 4
            ) as inversion_rate_cte,
            cte_uom.hierarchy + 1 as hierarchy
        from uom src
        join
            cte_uom
            on src.item_guid = cte_uom.item_guid
            and cte_uom.to_uom = src.from_uom
        /*and cte_uom.to_uom <> src.to_uom
            and cte_uom._uom <> cte_uom.to_uom*/
        where cte_uom.hierarchy <= 5  -- as there is no limit how many times the loop will execute keeping the loop execution limit to 5
    )
select
    cast(item_guid as text(255)) as item_guid,
    cast(substring(from_uom, 1, 255) as text(255)) as from_uom,
    cast(substring(to_uom, 1, 255) as text(255)) as to_uom,
    cast(conversion_rate as number(38, 10)) as conversion_rate,
    cast(inversion_rate as number(38, 10)) as inversion_rate,
    cast(substring(hierarchy, 1, 255) as text(255)) as hierarchy
from cte_uom
-- where item_guid = '1138940011'
qualify
    row_number() over (partition by item_guid, from_uom, to_uom order by hierarchy) = 1
order by hierarchy