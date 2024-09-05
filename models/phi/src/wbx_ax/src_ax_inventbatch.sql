

with source as (

    select * from {{ source('WEETABIX', 'inventbatch') }}

),

renamed as (

    select
        inventbatchid,
        expdate,
        itemid,
        proddate,
        description,
        pdsbestbeforedate,
        pdscountryoforigin1,
        pdscountryoforigin2,
        pdsdispositioncode,
        pdsfinishedgoodsdatetested,
        pdsinheritbatchattrib,
        pdsinheritedshelflife,
        pdssamelot,
        pdsshelfadvicedate,
        pdsusevendbatchdate,
        pdsusevendbatchexp,
        pdsvendbatchdate,
        pdsvendbatchid,
        pdsvendexpirydate,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
