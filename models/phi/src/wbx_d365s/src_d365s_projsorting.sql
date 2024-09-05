

with source as (

    select * from {{ source('D365S', 'projsorting') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        sortingid,
        description,
        sortcode,
        upper(dataareaid) as dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
