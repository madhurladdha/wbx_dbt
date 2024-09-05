

with source as (

    select * from {{ source('WEETABIX', 'wbxsocancelreasontable') }}

),

renamed as (

    select
        reasoncode,
        reasoncomments,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
