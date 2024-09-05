

with source as (

    select * from {{ source('WEETABIX', 'purchparmline') }}

),

renamed as (

    select
        remainbeforeinvent,
        remainafterinvent,
        inventnow,
        remainafter,
        remainbefore,
        receivenow,
        linenum,
        itemid,
        deliveryname,
        linepercent,
        purchmarkup,
        priceunit,
        multilndisc,
        linedisc,
        purchprice,
        multilnpercent,
        lineamount,
        orderaccount,
        purchlinerecid,
        invoiceaccount,
        parmid,
        closed,
        changedmanually,
        currencycode,
        inventtransid,
        inventdimid,
        origpurchid,
        invoiceinfotablerefid,
        tablerefid,
        tax1099amount,
        tax1099state,
        tax1099stateamount,
        taxitemgroup,
        taxgroup,
        description,
        reasontableref,
        procurementcategory,
        remainbeforeinventphysical,
        tax1099fields,
        deliverypostaladdress,
        purchaselinelinenumber,
        previousreceivenow,
        previousinventnow,
        postingprofile_ru,
        inventprofiletype_ru,
        customsimportinvoicenumtbl_in,
        customsbillofentrynumtbl_in,
        receivedqty_in,
        acceptedqty_in,
        rejectedqty_in,
        assessablevalue_in,
        maximumretailprice_in,
        customsinvoiceregnrecid_in,
        deviationqty_ru,
        cfoptable_br,
        pdscountryoforigin1,
        pdscountryoforigin2,
        pdscwpreviousreceivenow,
        pdscwreceivenow,
        pdscwremainafterinvent,
        pdscwremainbeforeinvent,
        pdsusevendbatchdate,
        pdsusevendbatchexp,
        pdsvendbatchdate,
        pdsvendbatchid,
        pdsvendexpirydate,
        previousdeviationqty,
        taxservicecode_br,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed