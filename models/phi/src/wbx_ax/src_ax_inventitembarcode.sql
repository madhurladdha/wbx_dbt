with
    source as (select * from {{ source("WEETABIX", "inventitembarcode") }}),

    renamed as (

        select
            itembarcode,
            itemid,
            inventdimid,
            barcodesetupid,
            useforprinting,
            useforinput,
            description,
            qty,
            unitid,
            retailvariantid,
            retailshowforitem,
            blocked,
            modifieddatetime,
            del_modifiedtime,
            modifiedby,
            dataareaid,
            recversion,
            partition,
            recid

        from source

    )

select *
from renamed
