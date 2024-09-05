with
    source as (

        select * from {{ source("SHAREPOINT_DSCI", "wtx_item_categorization") }}

    ),

    renamed as (

        select
            _line, item_code, item_id, item_type, buyer_code, company, _fivetran_synced

        from source

    )

select *
from renamed
