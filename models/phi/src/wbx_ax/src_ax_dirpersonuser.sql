with source as (

    select * from {{ source('WEETABIX', 'dirpersonuser') }}

),

renamed as (

    select
        personparty,
        user_,
        validto,
        validtotzid,
        validfrom,
        validfromtzid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed