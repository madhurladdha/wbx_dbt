{{
    config(
        materialized="view",
        tags=["redzone", "OEE", "v_productunitconversion"],
    )
}}

with
    source as (select * from {{ source("weetabix-org", "v_productunitconversion") }}),

    renamed as (

        select
            "siteId",
            "productTypeUUID",
            "productTypeName",
            "productTypeSku",
            "unitConversionUUID",
            "fromValue",
            "fromUOMUUID",
            "fromUOMName",
            "toValue",
            "toUOMUUID",
            "toUOMName",
            "unitConversionDeactivated",
            "siteUUID",
            "siteName"

        from source

    )

select *
from renamed
