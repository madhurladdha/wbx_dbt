with
    source as (select * from {{ source("SHAREPOINT_DSCI", "wtx_item_inflation") }}),

    renamed as (

        select
            _line,
            item_buyergroup,
            year,
            scenario,
            oct,
            nov,
            dec,
            jan,
            feb,
            mar,
            apr,
            may,
            jun,
            jul,
            aug,
            sep,
            _fivetran_synced

        from source

    )

select *
from renamed
