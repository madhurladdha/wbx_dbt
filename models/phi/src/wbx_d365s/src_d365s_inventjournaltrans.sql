with
d365_source as (
    select *
    from {{ source("D365S", "inventjournaltrans") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        journalid as journalid,
        linenum as linenum,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        voucher as voucher,
        journaltype as journaltype,
        itemid as itemid,
        qty as qty,
        costprice as costprice,
        priceunit as priceunit,
        costmarkup as costmarkup,
        costamount as costamount,
        salesamount as salesamount,
        null as projtransid,
        inventtransid as inventtransid,
        null as inventtransidfather,
        inventonhand as inventonhand,
        counted as counted,
        bomline as bomline,
        null as inventtransidreturn,
        null as projcategoryid,
        null as projid,
        toinventtransid as toinventtransid,
        reasonrefrecid as reasonrefrecid,
        inventdimid as inventdimid,
        toinventdimid as toinventdimid,
        null as reqpoid,
        assettranstype as assettranstype,
        null as assetid,
        null as assetbookid,
        null as projtaxgroupid,
        null as projsalescurrencyid,
        null as projlinepropertyid,
        null as projtaxitemgroupid,
        null as projunitid,
        projsalesprice as projsalesprice,
        inventreftype as inventreftype,
        null as inventrefid,
        null as inventreftransid,
        profitset as profitset,
        null as activitynumber,
        cast(releasedate as TIMESTAMP_NTZ) as releasedate,
        null as releasedatetzid,
        ledgerdimension as ledgerdimension,
        worker as worker,
        defaultdimension as defaultdimension,
        null as excisetariffcodes_in,
        null as excisetype_in,
        null as exciserecordtype_in,
        null as dsa_in,
        storno_ru as storno_ru,
        cast(intrastatfulfillmentdate_hu as TIMESTAMP_NTZ)
            as intrastatfulfillmentdate_hu,
        null as scraptypeid_ru,
        null as retailinfocodeidex2,
        null as retailinformationsubcodeidex2,
        pdscopybatchattrib as pdscopybatchattrib,
        pdscwinventonhand as pdscwinventonhand,
        pdscwinventqtycounted as pdscwinventqtycounted,
        pdscwqty as pdscwqty,
        null as postaladdress_in,
        null as warehouselocation_in,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed