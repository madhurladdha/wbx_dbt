with source as (

    select *
    from {{ source('D365', 'eco_res_product_instance_value') }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        recid,
        product

    from source

)

select * from renamed