

with source as (

    select * from {{ source('WEETABIX', 'whsworkquarantine') }}

),

renamed as (

    select
        workid,
        linenum,
        itemid,
        inventtransidfrom,
        inventtransidto,
        qty,
        inventqtyremain,
        inventdimidfrom,
        inventdimidto,
        transdatetime,
        transdatetimetzid,
        modifieddatetime,
        modifiedby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxreasoncode

    from source

)

select * from renamed
