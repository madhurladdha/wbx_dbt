with d365source as (

    select *
    from {{ source("D365S", "purchtable") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(trim(dataareaid)) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (
    select
        'D365S' as source,
        purchid as purchid,
        purchname as purchname,
        orderaccount as orderaccount,
        invoiceaccount as invoiceaccount,
        null as freightzone,
        email as email,
        cast(deliverydate as TIMESTAMP_NTZ) as deliverydate,
        deliverytype as deliverytype,
        addressrefrecid as addressrefrecid,
        addressreftableid as addressreftableid,
        intercompanyoriginalsalesid as intercompanyoriginalsalesid,
        null as intercompanyoriginalcustacct,
        currencycode as currencycode,
        payment as payment,
        null as cashdisc,
        null as countyorigdest,
        intercompanydirectdelivery as intercompanydirectdelivery,
        vendgroup as vendgroup,
        null as linedisc,
        discpercent as discpercent,
        null as pricegroupid,
        null as multilinedisc,
        null as enddisc,
        null as intercocustpurchorderformnum,
        taxgroup as taxgroup,
        dlvterm as dlvterm,
        dlvmode as dlvmode,
        purchstatus as purchstatus,
        null as markupgroup,
        purchasetype as purchasetype,
        null as url,
        postingprofile as postingprofile,
        null as transactioncode,
        null as enterprisenumber,
        settlevoucher as settlevoucher,
        null as intercoallowindirectcreation,
        intercompanyorigin as intercompanyorigin,
        cashdiscpercent as cashdiscpercent,
        deliveryname as deliveryname,
        covstatus as covstatus,
        null as paymentsched,
        inventsiteid as inventsiteid,
        onetimevendor as onetimevendor,
        returnitemnum as returnitemnum,
        freightsliptype as freightsliptype,
        documentstatus as documentstatus,
        null as contactpersonid,
        inventlocationid as inventlocationid,
        null as bankcentralbankpurposecode,
        itembuyergroupid as itembuyergroupid,
        null as projid,
        purchpoolid as purchpoolid,
        vatnum as vatnum,
        null as port,
        incltax as incltax,
        null as bankcentralbankpurposetext,
        null as numbersequencegroup,
        languageid as languageid,
        autosummarymoduletype as autosummarymoduletype,
        null as transport,
        paymmode as paymmode,
        null as paymspec,
        cast(fixedduedate as TIMESTAMP_NTZ) as fixedduedate,
        null as statprocid,
        vendorref as vendorref,
        intercompanycompanyid as intercompanycompanyid,
        intercompanysalesid as intercompanysalesid,
        intercompanyorder as intercompanyorder,
        null as returnreasoncodeid,
        returnreplacementcreated as returnreplacementcreated,
        null as reqattention,
        defaultdimension as defaultdimension,
        cast(confirmeddlvearliest as TIMESTAMP_NTZ) as confirmeddlvearliest,
        null as contractnum_sa,
        changerequestrequired as changerequestrequired,
        reasontableref as reasontableref,
        documentstate as documentstate,
        ismodified as ismodified,
        matchingagreement as matchingagreement,
        systementrysource as systementrysource,
        systementrychangepolicy as systementrychangepolicy,
        manualentrychangepolicy as manualentrychangepolicy,
        vendinvoicedeclaration_is as vendinvoicedeclaration_is,
        workerpurchplacer as workerpurchplacer,
        deliverypostaladdress as deliverypostaladdress,
        bankdocumenttype as bankdocumenttype,
        listcode as listcode,
        isencumbrancerequired as isencumbrancerequired,
        sourcedocumentline as sourcedocumentline,
        sourcedocumentheader as sourcedocumentheader,
        requester as requester,
        accountingdistributiontemplate as accountingdistributiontemplate,
        cast(accountingdate as TIMESTAMP_NTZ) as accountingdate,
        cast(finalizeclosingdate as TIMESTAMP_NTZ) as finalizeclosingdate,
        constarget_jp as constarget_jp,
        cast(intrastatfulfillmentdate_hu as TIMESTAMP_NTZ)
            as intrastatfulfillmentdate_hu,
        unitedvatinvoice_lt as unitedvatinvoice_lt,
        intrastataddvalue_lv as intrastataddvalue_lv,
        null as invoiceregister_lt,
        null as packingslipregister_lt,
        null as servicename,
        null as serviceaddress,
        onetimesupplier as onetimesupplier,
        cast(servicedate as TIMESTAMP_NTZ) as servicedate,
        cast(confirmeddlv as TIMESTAMP_NTZ) as confirmeddlv,
        fshautocreated as fshautocreated,
        cast(crossdockingdate as TIMESTAMP_NTZ) as crossdockingdate,
        null as servicecategory,
        cast(availsalesdate as TIMESTAMP_NTZ) as availsalesdate,
        cast(localdeliverydate as TIMESTAMP_NTZ) as localdeliverydate,
        replenishmentlocation as replenishmentlocation,
        retailretailstatustype as retailretailstatustype,
        null as retaildriverdetails,
        null as retailconcessionpurch,
        confirmingpo as confirmingpo,
        mcrdropshipment as mcrdropshipment,
        cast(exchangeratedate as TIMESTAMP_NTZ) as exchangeratedate,
        fixedexchrate as fixedexchrate,
        null as tamvendrebategroupid,
        null as taxperiodpaymentcode_pl,
        transportationdocument as transportationdocument,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxinvoicehandling,
        null as bisedidelivery,
        null as bisediprocess
    from d365source

)

select *
from renamed
