
with source as (

    select *
    from {{ source('D365', 'eco_res_text_value') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        recid,
        text_value,
        last_processed_change_date_time,
        lsn

    from source

)

select * from renamed