{{
    config(
        tags=["ax_hist_fact","ax_hist_sales","ax_hist_on_demand"]
    )
}}


with
    source as (select * from {{ source("FACTS_FOR_COMPARE", "sls_wtx_epos_fact") }}),

    renamed as (

        select
            source_system,
            item_guid,
            source_item_identifier,
            trade_type_code,
            bill_source_customer_code,
            calendar_week,
            calendar_date,
            fiscal_period_number,
            primary_uom,
            bill_customer_address_guid,
            qty_ca,
            qty_kg,
            qty_ul,
            qty_prim,
            number_of_days

        from source

    )

select *
,  {{dbt_utils.surrogate_key(
                    [
                        "source_system",
                        "source_item_identifier",
                        "bill_source_customer_code",
                        "calendar_date",
                    ]
                )
            }} as unique_key
from renamed
