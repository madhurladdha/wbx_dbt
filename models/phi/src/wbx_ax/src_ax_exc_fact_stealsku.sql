with
    source as (select * from {{ source("WEETABIX", "EXC_Fact_StealSku") }}),

    renamed as (select cust_idx, std_sku_idx, promo_sku_idx, cannibpc from source)

select *
from renamed
