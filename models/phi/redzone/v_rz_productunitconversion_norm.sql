{{
    config(
        materialized="view",
        tags=["redzone", "OEE", "v_productunitconversion"],
    )
}}

with conv as (select * from {{ ref("src_rz_v_productunitconversion") }})

select
    "siteUUID",
    "siteName",
    "productTypeUUID",
    "productTypeName",
    case
        when "fromUOMName" in ('LBS', 'LB', 'Kgs')
        then "fromUOMName"
        when "toUOMName" in ('LBS', 'LB', 'Kgs')
        then "toUOMName"
    end as touomname_norm,
    case
        when "fromUOMName" in ('LBS', 'LB', 'Kgs')
        then "toUOMName"
        when "toUOMName" in ('LBS', 'LB', 'Kgs')
        then "fromUOMName"
    end as fromuomname_norm,
    case
        when "fromUOMName" in ('LBS', 'LB', 'Kgs')
        then "toUOMUUID"
        when "toUOMName" in ('LBS', 'LB', 'Kgs')
        then "fromUOMUUID"
    end as fromuomuuid_norm,
    case
        when "fromUOMName" in ('LBS', 'LB', 'Kgs')
        then "fromValue" / "toValue"
        when "toUOMName" in ('LBS', 'LB', 'Kgs')
        then "toValue" / "fromValue"
    end as tovalue_norm
from conv
where ("fromUOMName" in ('LBS', 'LB', 'Kgs') or "toUOMName" in ('LBS', 'LB', 'Kgs'))
