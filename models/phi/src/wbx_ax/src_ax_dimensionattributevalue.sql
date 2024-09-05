

with source as (

    select * from {{ source('WEETABIX', 'dimensionattributevalue') }}

),

renamed as (

    select
        dimensionattribute,
        issuspended,
        activefrom,
        activeto,
        istotal,
        entityinstance,
        isblockedformanualentry,
        groupdimension,
        hashkey,
        isdeleted,
        owner,
        isbalancing_psn,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
