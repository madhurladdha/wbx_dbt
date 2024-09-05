

with source as (

    select * from {{ source('WEETABIX', 'forecastitemallocationline') }}

),

renamed as (

    select
        allocationid,
        linenum,
        itemid,
        percent_,
        inventdimid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
