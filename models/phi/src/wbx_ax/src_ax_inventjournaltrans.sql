

with source as (

    select * from {{ source('WEETABIX', 'inventjournaltrans') }}

),

renamed as (

    select
        journalid,
        linenum,
        transdate,
        voucher,
        journaltype,
        itemid,
        qty,
        costprice,
        priceunit,
        costmarkup,
        costamount,
        salesamount,
        projtransid,
        inventtransid,
        inventtransidfather,
        inventonhand,
        counted,
        bomline,
        inventtransidreturn,
        projcategoryid,
        projid,
        toinventtransid,
        reasonrefrecid,
        inventdimid,
        toinventdimid,
        reqpoid,
        assettranstype,
        assetid,
        assetbookid,
        projtaxgroupid,
        projsalescurrencyid,
        projlinepropertyid,
        projtaxitemgroupid,
        projunitid,
        projsalesprice,
        inventreftype,
        inventrefid,
        inventreftransid,
        profitset,
        activitynumber,
        releasedate,
        releasedatetzid,
        ledgerdimension,
        worker,
        defaultdimension,
        excisetariffcodes_in,
        excisetype_in,
        exciserecordtype_in,
        dsa_in,
        storno_ru,
        intrastatfulfillmentdate_hu,
        scraptypeid_ru,
        retailinfocodeidex2,
        retailinformationsubcodeidex2,
        pdscopybatchattrib,
        pdscwinventonhand,
        pdscwinventqtycounted,
        pdscwqty,
        postaladdress_in,
        warehouselocation_in,
        modifieddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
