

with source as (

    select * from {{ source('D365', 'proj_sorting') }}
    where _FIVETRAN_DELETED='FALSE'
),

renamed as (

    select
        sorting_id as sortingid,
        description as description,
        sort_code as sortcode,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from source

)

select * from renamed
