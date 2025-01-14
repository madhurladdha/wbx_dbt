

with source as (

    select * from {{ source('WEETABIX', 'inventtable') }}

),

renamed as (

    select
        itemid,
        itemtype,
        purchmodel,
        height,
        width,
        salesmodel,
        costgroupid,
        reqgroupid,
        epcmanager,
        primaryvendorid,
        netweight,
        depth,
        unitvolume,
        bomunitid,
        itempricetolerancegroupid,
        density,
        costmodel,
        usealtitemid,
        altitemid,
        matchingpolicy,
        intracode,
        prodflushingprincip,
        minimumpalletquantity,
        pbaitemautogenerated,
        wmsarrivalhandlingtime,
        bommanualreceipt,
        phantom,
        intraunit,
        bomlevel,
        batchnumgroupid,
        autoreportfinished,
        origcountryregionid,
        statisticsfactor,
        altconfigid,
        standardconfigid,
        prodpoolid,
        propertyid,
        abctieup,
        abcrevenue,
        abcvalue,
        abccontributionmargin,
        commissiongroupid,
        salespercentmarkup,
        salescontributionratio,
        salespricemodelbasic,
        namealias,
        prodgroupid,
        projcategoryid,
        grossdepth,
        grosswidth,
        grossheight,
        standardpalletquantity,
        qtyperlayer,
        sortcode,
        serialnumgroupid,
        itembuyergroupid,
        taxpackagingqty,
        wmspallettypeid,
        origstateid,
        wmspickingqtytime,
        taraweight,
        packaginggroupid,
        scrapvar,
        scrapconst,
        standardinventcolorid,
        standardinventsizeid,
        itemdimcostprice,
        altinventsizeid,
        altinventcolorid,
        forecastdmpinclude,
        product,
        pallettagging,
        itemtagginglevel,
        defaultdimension,
        fiscallifoavoidcalc,
        fiscallifonormalvalue,
        fiscallifonormalvaluecalc,
        bomcalcgroupid,
        inventfiscallifogroup,
        ngpcodestable_fr,
        origcountyid,
        taxfiscalclassification_br,
        pbaitemconfigurable,
        pbainventitemgroupid,
        pbahidedialog,
        pbahideapproval,
        pbaautostart,
        pbamandatoryconfig,
        inventproducttype_br,
        taxationorigin_br,
        taxservicecode_br,
        excisetariffcodes_in,
        customsexporttariffcodetbl_in,
        customsimporttariffcodetbl_in,
        servicecodetable_in,
        eximproductgrouptable_in,
        packing_ru,
        assetgroupid_ru,
        assetid_ru,
        intrastatexclude,
        intrastatprocid_cz,
        pkwiucode_pl,
        exceptioncode_br,
        icmsonservice_br,
        pdscwwmsstandardpalletqty,
        pdscwwmsminimumpalletqty,
        pdscwwmsqtyperlayer,
        alcoholmanufacturerid_ru,
        alcoholproductiontypeid_ru,
        alcoholstrength_ru,
        altinventstyleid,
        approxtaxvalue_br,
        batchmergedatecalcmethod,
        daxintegrationkey,
        markupcode_ru,
        nrtaxgroup_lv,
        pdsbaseattributeid,
        pdsbestbefore,
        pdsfreightallocationgroupid,
        pdsitemrebategroupid,
        pdspotencyattribrecording,
        pdsshelfadvice,
        pdsshelflife,
        pdstargetfactor,
        pdsvendorcheckitem,
        pmfplanningitemid,
        pmfproducttype,
        pmfyieldpct,
        sadratecode_pl,
        skipintracompanysync_ru,
        standardinventstyleid,
        dsa_in,
        exciserecordtype_in,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        createddatetime,
        del_createdtime,
        createdby,
        dataareaid,
        recversion,
        partition,
        recid,
        wbxautomaticformulachange,
        wbxcycletimessc,
        serviceaccountingcodetable_in,
        exempt_in,
        hsncodetable_in,
        includedemandforecast,
        avpweight,
        consumerunit,
        currentflag,
        inventbrandingcodeid,
        inventpacksizecodeid,
        inventproductclasscodeid,
        inventstrategiccodeid,
        inventsubproductcodeid,
        pmpflag,
        avpflag,
        satcodeid_mx,
        undershelflife,
        overshelflife

    from source

)

select * from renamed
