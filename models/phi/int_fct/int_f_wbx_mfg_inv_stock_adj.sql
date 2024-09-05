{{ config(tags=["wbx", "manufacturing", "yield", "stock_adj", "inventory"]) }}

with stage as (select * from {{ ref("stg_f_wbx_mfg_inv_stock_adj") }})

select
    *,
    {{ dbt_utils.surrogate_key(["source_system", "source_item_identifier"]) }}
    as item_guid,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "source_business_unit_code",
                "'PLANT_DC'",
            ]
        )
    }} as plantdc_address_guid,
    {{
        dbt_utils.surrogate_key(
            [
                "source_system",
                "source_account_identifier",
                "source_record_id",
                "company_code",
                "source_item_identifier",
                "source_business_unit_code",
                "gl_date",
            ]
        )
    }} as unique_key
from stage