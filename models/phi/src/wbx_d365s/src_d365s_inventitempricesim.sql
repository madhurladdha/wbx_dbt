with
d365_source as (
    select *
    from {{ source("D365S", "inventitempricesim") }}
    where
        upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
        and _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        itemid as itemid,
        cast(versionid as varchar(255)) as versionid,
        cast(fromdate as timestamp_ntz) as fromdate,
        pricetype as pricetype,
        inventdimid as inventdimid,
        markup as markup,
        priceunit as priceunit,
        price as price,
        pricecalcid as pricecalcid,
        unitid as unitid,
        priceallocatemarkup as priceallocatemarkup,
        priceqty as priceqty,
        priceseccur_ru as priceseccur_ru,
        markupseccur_ru as markupseccur_ru,
        cast(modifieddatetime as timestamp_ntz) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
