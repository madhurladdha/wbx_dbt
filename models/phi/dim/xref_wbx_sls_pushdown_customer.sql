{{ config(materialized=env_var("DBT_MAT_TABLE")) }}

with
    source as (select * from {{ source("EI_RDM", "sls_wtx_cust_pushdown_xref") }}),

    xref_wbx_customer_pushdown as (

        select
            source_system,
            trade_type_code,
            bill_source_customer_code,
            {{
                dbt_utils.surrogate_key(
                    [
                        "source.source_system",
                        "source.bill_source_customer_code",
                        "'CUSTOMER_MAIN'",
                    ]
                )
            }} as bill_customer_address_guid

        from source

    )

select
    cast(substring(source_system, 1, 10) as text(10)) as source_system,
    cast(substring(trade_type_code, 1, 60) as text(60)) as trade_type_code,
    cast( substring(bill_source_customer_code, 1, 255) as text(255) ) as bill_source_customer_code,
    cast(bill_customer_address_guid as text(255)) as bill_customer_address_guid
from xref_wbx_customer_pushdown
