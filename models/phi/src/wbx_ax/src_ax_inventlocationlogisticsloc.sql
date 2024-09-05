

with source as (

    select * from {{ source('WEETABIX', 'inventlocationlogisticsloc') }}

),

renamed as (

    select
        inventlocation,
        location,
        attentiontoaddressline,
        isdefault,
        isprimary,
        ispostaladdress,
        isprivate,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
