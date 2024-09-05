with
d365_source as (
    select *
    from {{ source("D365S", "dirpersonuser") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        personparty as personparty,
        user as user_,
        cast(validto as TIMESTAMP_NTZ) as validto,
        null as validtotzid,
        cast(validfrom as TIMESTAMP_NTZ) as validfrom,
        null as validfromtzid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed