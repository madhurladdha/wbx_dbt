with
    source as (select * from {{ source("EI_RDM", "sls_wtx_cust_pushdown_xref") }}),

    renamed as (

        select
            source_system,
            trade_type_code,
            bill_source_customer_code,
            bill_customer_address_guid

        from source

    )

select *
from renamed
