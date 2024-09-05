with
d365_source as (
    select *
    from {{ source("D365", "dir_person_user") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        person_party as personparty,
        user_ as user_,
        valid_to as validto,
        validtotzid as validtotzid,
        valid_from as validfrom,
        validfromtzid as validfromtzid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed