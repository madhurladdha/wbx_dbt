with source as (

    select * from {{ source('WEETABIX', 'wrkctractivityrequirementset') }}

),

renamed as (

    select
        activity,
        quantity,
        loadpercent,
        description,
        recversion,
        partition,
        recid

    from source

)

select * from renamed