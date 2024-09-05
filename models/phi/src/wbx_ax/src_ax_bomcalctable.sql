

with source as (

    select * from {{ source('WEETABIX', 'bomcalctable') }}

),

renamed as (

    select
        itemid,
        transdate,
        qty,
        costprice,
        costmarkup,
        salesprice,
        salesmarkup,
        unitid,
        profitset,
        bomid,
        routeid,
        pricecalcid,
        inventdimid,
        netweight,
        leanproductionflowreference,
        bomcalctype,
        costpriceseccur_ru,
        costmarkupseccur_ru,
        costcalculationmethod,
        pmfbomversion,
        pmfparentcalcid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
