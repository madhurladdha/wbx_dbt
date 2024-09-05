with d365_source as (

    select *
    from {{ source("D365S", "bomcalctable") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        qty as qty,
        costprice as costprice,
        costmarkup as costmarkup,
        salesprice as salesprice,
        salesmarkup as salesmarkup,
        unitid as unitid,
        profitset as profitset,
        bomid as bomid,
        routeid as routeid,
        pricecalcid as pricecalcid,
        inventdimid as inventdimid,
        netweight as netweight,
        leanproductionflowreference as leanproductionflowreference,
        bomcalctype as bomcalctype,
        costpriceseccur_ru as costpriceseccur_ru,
        costmarkupseccur_ru as costmarkupseccur_ru,
        costcalculationmethod as costcalculationmethod,
        pmfbomversion as pmfbomversion,
        null as pmfparentcalcid,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source
    where upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed