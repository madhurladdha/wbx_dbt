

with source as (

    select * from {{ source('WEETABIX', 'MDS_TBLDIMBRANCHCHG') }}

),

renamed as (

    select
        id,
        muid,
        versionname,
        versionnumber,
        versionflag,
        name,
        code,
        changetrackingmask,
        moddate,
        startdate,
        enddate,
        kcnataccount,
        knataccount,
        kctradesector,
        ktradesector,
        tradesector,
        tradesectorname_code,
        tradesectorname_name,
        tradesectorname_id,
        kcmarketcode,
        kmarketcode,
        marketcode,
        marketcodename_code,
        marketcodename_name,
        marketcodename_id,
        kcsubmarketcode,
        ksubmarketcode,
        submarketcode,
        submarketname_code,
        submarketname_name,
        submarketname_id,
        kctradeclass,
        ktradeclass,
        tradeclass,
        tradeclassname_code,
        tradeclassname_name,
        tradeclassname_id,
        kctradegroup,
        ktradegroup,
        tradegroup,
        tradegroupname_code,
        tradegroupname_name,
        tradegroupname_id,
        kctradetype,
        ktradetype,
        tradetype,
        tradetypename_code,
        tradetypename_name,
        tradetypename_id,
        kcaccount,
        kaccount,
        customeraccount,
        customername,
        db_branchcode,
        db_territorycode,
        db_branchregion,
        db_branchdivision,
        db_closedind,
        db_deliveryname,
        db_deliverytown,
        db_deliveryaddr1,
        db_deliveryaddr2,
        db_deliveryaddr3,
        db_deliveryaddr4,
        db_deliveryaddr5,
        db_deliverypostcode,
        db_custbranchref,
        db_invoicename,
        db_invoiceaddr1,
        db_invoiceaddr2,
        db_invoiceaddr3,
        db_invoiceaddr4,
        db_invoiceaddr5,
        db_invoicepostcode,
        db_noneupricesind,
        db_foreigncurrency,
        db_eucountrycode,
        db_euinvoicecountry,
        db_anaorderlocation,
        db_exclind,
        kcustomerdimension,
        db_anainvoicelocation,
        ksalestradeclass,
        ksalestradegroup,
        ksalestradetype,
        customerhierarchy_code,
        customerhierarchy_name,
        customerhierarchy_id,
        enterdatetime

    from source

)

select * from renamed