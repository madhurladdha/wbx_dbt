with d365_source as (
    select *
    from {{ source("D365", "wrk_ctr_activity") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        entity_type as entitytype,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed