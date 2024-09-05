with
d365_source as (
    select *
    from {{ source("D365S", "inventtransoriginprodtable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(
            prodorderdataareaid
        ) in

        {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        prodorderid as prodorderid,
        inventtransorigin as inventtransorigin,
        upper(prodorderdataareaid) as prodorderdataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed