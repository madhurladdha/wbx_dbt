

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_ROB_Impact') }}

),

renamed as (

    select
        impact_idx,
        impact_code,
        impact_name,
        robsubtype_idx,
        sortorder

    from source

)

select * from renamed
