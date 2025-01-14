with source as (

    select * from {{ source('WEETABIX', 'custtable') }}

),

renamed as (

    select
        accountnum,
        invoiceaccount,
        custgroup,
        linedisc,
        paymtermid,
        cashdisc,
        currency,
        intercompanyautocreateorders,
        salesgroup,
        blocked,
        onetimecustomer,
        accountstatement,
        creditmax,
        mandatorycreditlimit,
        vendaccount,
        pricegroup,
        multilinedisc,
        enddisc,
        vatnum,
        inventlocation,
        dlvterm,
        dlvmode,
        markupgroup,
        clearingperiod,
        freightzone,
        creditrating,
        taxgroup,
        statisticsgroup,
        paymmode,
        commissiongroup,
        bankaccount,
        paymsched,
        contactpersonid,
        invoiceaddress,
        ouraccountnum,
        salespoolid,
        incltax,
        custitemgroupid,
        numbersequencegroup,
        paymdayid,
        lineofbusinessid,
        destinationcodeid,
        girotype,
        suppitemgroupid,
        girotypeinterestnote,
        taxlicensenum,
        websalesorderdisplay,
        paymspec,
        bankcentralbankpurposetext,
        bankcentralbankpurposecode,
        intercoallowindirectcreation,
        packmaterialfeelicensenum,
        taxbordernumber_fi,
        einvoiceeannum,
        fiscalcode,
        dlvreason,
        forecastdmpinclude,
        girotypecollectionletter,
        salescalendarid,
        custclassificationid,
        intercompanydirectdelivery,
        enterprisenumber,
        shipcarrieraccount,
        girotypeprojinvoice,
        inventsiteid,
        orderentrydeadlinegroupid,
        shipcarrierid,
        shipcarrierfuelsurcharge,
        shipcarrierblindshipment,
        shipcarrieraccountcode,
        girotypefreetextinvoice,
        syncentityid,
        syncversion,
        memo,
        salesdistrictid,
        segmentid,
        subsegmentid,
        rfiditemtagging,
        rfidcasetagging,
        rfidpallettagging,
        companychainid,
        companyidsiret,
        party,
        identificationnumber,
        partycountry,
        partystate,
        orgid,
        paymidtype,
        factoringaccount,
        defaultdimension,
        custexcludecollectionfee,
        custexcludeinterestcharges,
        companynafcode,
        bankcustpaymidtable,
        girotypeaccountstatement,
        maincontactworker,
        creditcardaddressverification,
        creditcardcvc,
        creditcardaddressverivoid,
        creditcardaddressverilevel,
        companytype_mx,
        rfc_mx,
        curp_mx,
        stateinscription_mx,
        residenceforeignctryregid_it,
        birthcountycode_it,
        birthdate_it,
        birthplace_it,
        einvoice,
        einvoiceregister_it,
        ccmnum_br,
        cnpjcpfnum_br,
        pbacustgroupid,
        ienum_br,
        suframanumber_br,
        suframa_br,
        custfinaluser_br,
        interestcode_br,
        finecode_br,
        suframapiscofins_br,
        taxwithholdcalculate_th,
        taxwithholdgroup_th,
        consday_jp,
        nit_br,
        insscei_br,
        cnae_br,
        icmscontributor_br,
        servicecodeondlvaddress_br,
        inventprofiletype_ru,
        inventprofileid_ru,
        taxwithholdcalculate_in,
        unitedvatinvoice_lt,
        foreignerid_br,
        enterprisecode,
        commercialregistersection,
        commercialregisterinsetnumber,
        commercialregister,
        regnum_w,
        isresident_lv,
        intbank_lv,
        paymentreference_ee,
        packagedepositexcempt_pl,
        fednonfedindicator,
        irs1099cindicator,
        agencylocationcode,
        federalcomments,
        usepurchrequest,
        mcrmergedparent,
        mcrmergedroot,
        affiliated_ru,
        cashdiscbasedays,
        custtradingpartnercode,
        custwhtcontributiontype_br,
        daxintegrationid,
        defaultdirectdebitmandate,
        defaultinventstatusid,
        entrycertificaterequired_w,
        exportsales_pl,
        expressbilloflading,
        fiscaldoctype_pl,
        foreignresident_ru,
        generateincomingfiscaldoc_br,
        invoicepostingtype_ru,
        issueownentrycertificate_w,
        issuercountry_hu,
        lvpaymtranscodes,
        mandatoryvatdate_pl,
        passportno_hu,
        pdscustrebategroupid,
        pdsfreightaccrued,
        pdsrebatetmagroup,
        taxperiodpaymentcode_pl,
        usecashdisc,
        authorityoffice_it,
        presencetype_br,
        taxgstreliefgroupheading_my,
        modifieddatetime,
        del_modifiedtime,
        modifiedby,
        createddatetime,
        del_createdtime,
        upper(trim(dataareaid)) as dataareaid,
        recversion,
        partition,
        recid,
        foreigntaxregistration_mx,
        biseancodeid,
        bisedipartner,
        bisitemcoding,
        bisitemcodingbarcodesetupid,
        bisitemcodinggtinsetup,
        biseanbarcodesetupid,
        allowzeroprices,
        salestradegroupid,
        salestradesectorid,
        satpurpose_mx,
        satpaymmethod_mx

    from source

)

select * from renamed
