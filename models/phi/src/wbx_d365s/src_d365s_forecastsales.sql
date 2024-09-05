with
d365_source as (
    select *
    from {{ source("D365S", "forecastsales") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        cast(startdate as TIMESTAMP_NTZ) as startdate,
        cast(enddate as TIMESTAMP_NTZ) as enddate,
        freqcode as freqcode,
        active as active,
        inventqty as inventqty,
        salesprice as salesprice,
        discpercent as discpercent,
        comment as comment_,
        custgroupid as custgroupid,
        itemgroupid as itemgroupid,
        custaccountid as custaccountid,
        null as keyid,
        currency as currency,
        expandid as expandid,
        report as report,
        salesqty as salesqty,
        salesunitid as salesunitid,
        salesmarkup as salesmarkup,
        discamount as discamount,
        priceunit as priceunit,
        costprice as costprice,
        taxitemgroupid as taxitemgroupid,
        cov as cov,
        covstatus as covstatus,
        itemallocateid as itemallocateid,
        taxgroupid as taxgroupid,
        freq as freq,
        amount as amount,
        modelid as modelid,
        allocatemethod as allocatemethod,
        null as itembomid,
        null as itemrouteid,
        defaultdimension as defaultdimension,
        null as projid,
        null as projcategoryid,
        null as projlinepropertyid,
        inventdimid as inventdimid,
        null as projtransid,
        cast(projforecastsalespaymdate as TIMESTAMP_NTZ)
            as projforecastsalespaymdate,
        cast(projforecastcostpaymdate as TIMESTAMP_NTZ)
            as projforecastcostpaymdate,
        cast(projforecastinvoicedate as TIMESTAMP_NTZ)
            as projforecastinvoicedate,
        cast(projforecasteliminationdate as TIMESTAMP_NTZ)
            as projforecasteliminationdate,
        null as activitynumber,
        projforecastbudgettype as projforecastbudgettype,
        projfundingsource as projfundingsource,
        psarefpurchline as psarefpurchline,
        pdscwqty as pdscwqty,
        null as pdscwunitid,
        modifiedby as modifiedby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxforecastdifference,
        null as wbxforecastallocationpct
    from d365_source

)

select *
from renamed

