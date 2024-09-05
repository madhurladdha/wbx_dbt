

with source as (

    select * from {{ source('WEETABIX', 'whsworkinventtrans') }}

),

renamed as (

    select
        workid,
        linenum,
        inventtransidfrom,
        inventtransidto,
        itemid,
        inventdimidfrom,
        inventdimidto,
        inventtransidparent,
        qty,
        inventqtyremain,
        workhasreservation,
        transdatetime,
        transdatetimetzid,
        modifieddatetime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
