
with source as (

    select *
    from {{ source('D365S', 'ecorestextvalue') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365S' as source,
        recid as recid,
        textvalue as text_value,
        null as last_processed_change_date_time,
        null as lsn

    from source

)

select * from renamed