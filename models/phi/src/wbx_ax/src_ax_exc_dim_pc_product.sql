

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_PC_Product') }}

),

renamed as (

    select
        idx,
        code,
        name,
        inapplication,
        isnpd,
        isdisplayunit,
        date_inserted,
        date_updated

    from source

)

select * from renamed
