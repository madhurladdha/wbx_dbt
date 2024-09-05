

with source as (

    select * from {{ source('WEETABIX', 'vendtable') }}

),

renamed as (

    select
        accountnum,
        invoiceaccount,
        vendgroup,
        paymtermid,
        cashdisc,
        currency,
        linedisc,
        enterprisenumber,
        blocked,
        onetimevendor,
        pricegroup,
        multilinedisc,
        enddisc,
        paymid,
        vatnum,
        inventlocation,
        youraccountnum,
        dlvterm,
        dlvmode,
        bankaccount,
        paymmode,
        paymspec,
        markupgroup,
        clearingperiod,
        companyidsiret,
        taxgroup,
        freightzone,
        minorityowned,
        femaleowned,
        creditrating,
        creditmax,
        tax1099reports,
        tax1099regnum,
        paymsched,
        itembuyergroupid,
        contactpersonid,
        purchpoolid,
        purchamountpurchaseorder,
        incltax,
        venditemgroupid,
        numbersequencegroup,
        paymdayid,
        destinationcodeid,
        lineofbusinessid,
        suppitemgroupid,
        bankcentralbankpurposetext,
        bankcentralbankpurposecode,
        offsetaccounttype,
        purchcalendarid,
        organizationnumber,
        fiscalcode,
        taxwithholdcalculate,
        taxwithholdgroup,
        birthdate,
        birthplace,
        residenceforeigncntyregid,
        namecontrol,
        foreignentityindicator,
        taxidtype,
        dba,
        tax1099namechoice,
        secondtin,
        inventsiteid,
        segmentid,
        subsegmentid,
        companychainid,
        vendpricetolerancegroupid,
        memo,
        smallbusiness,
        locallyowned,
        bidonly,
        w9,
        orgid,
        factoringaccount,
        matchingpolicy,
        blockedreleasedate,
        blockedreleasedatetzid,
        w9included,
        vendexceptiongroup,
        party,
        defaultdimension,
        offsetledgerdimension,
        birthcountycode_it,
        heir_it,
        changerequestenabled,
        changerequestallowoverride,
        changerequestoverride,
        companynafcode,
        vendinvoicedeclaration_is,
        tax1099fields,
        vendortype_mx,
        foreigntaxregistration_mx,
        nationality_mx,
        diotcountrycode_mx,
        operationtype_mx,
        maincontactworker,
        companytype_mx,
        rfc_mx,
        curp_mx,
        stateinscription_mx,
        ccmnum_br,
        ienum_br,
        cnpjcpfnum_br,
        vendincomecode_br,
        nontaxable_br,
        vendconsumption_br,
        nit_br,
        insscei_br,
        cnae_br,
        icmscontributor_br,
        servicecodeondlvaddress_br,
        interestcode_br,
        finecode_br,
        vattaxagent_ru,
        taxwithholdvendortype_th,
        vatoperationcode_ru,
        vatpartnerkind_ru,
        inventprofiletype_ru,
        inventprofileid_ru,
        unitedvatinvoice_lt,
        consday_jp,
        structdepartment_ru,
        bankcentralbanktranstypecur_ru,
        bankorderofpayment_ru,
        enterprisecode,
        regnum_w,
        commercialregistersection,
        commercialregisterinsetnumber,
        commercialregister,
        isresident_lv,
        intbank_lv,
        ciscompanyregnum,
        cisnationalinsurancenum,
        cisstatus,
        cisuniquetaxpayerref,
        cisverificationdate,
        cisverificationnum,
        defaultinventstatusid,
        disabledowned,
        ethnicoriginid,
        foreignresident_ru,
        hubzone,
        ispaymfeecovered_jp,
        lvpaymtranscodes,
        mandatoryvatdate_pl,
        separatedivisionid_ru,
        tamrebategroupid,
        taxperiodpaymentcode_pl,
        usecashdisc,
        veteranowned,
        vendpaymfeegroup_jp,
        foreignerid_br,
        presencetype_br,
        vendorportaladministratorrecid,
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
        biseanbarcodesetupid,
        biseancodeid,
        bisedipartner,
        bisitemcoding,
        bisitemcodingbarcodesetupid,
        bisitemcodinggtinsetup,
        fatcafilingrequirement

    from source

)

select * from renamed
