

with source as (

    select * from {{ source('WEETABIX', 'dimensionattributevaluesetitem') }}

),

renamed as (

    select
        dimensionattributevalueset,
        dimensionattributevalue,
        displayvalue,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
