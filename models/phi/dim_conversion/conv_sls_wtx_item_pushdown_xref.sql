with
    source as (select * from {{ source("EI_RDM", "sls_wtx_item_pushdown_xref") }}),

    renamed as (

        select source_system, product_class_code, source_item_identifier, item_guid

        from source

    )

select *
from renamed
