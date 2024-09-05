with
d365_source as (
    select *
    from {{ source("D365", "dir_party_table") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        null as businessactivity_sa,
        null as businessactivitydesc_sa,
        null as filenumber_sa,
        companynafcode as companynafcode,
        null as businessnumber_ca,
        null as softwareidentificationcode_ca,
        null as fiscalcode_it,
        companytype_mx as companytype_mx,
        null as rfc_mx,
        null as curp_mx,
        null as stateinscription_mx,
        legalnature_it as legalnature_it,
        bank as bank,
        null as giro,
        null as regnum,
        coregnum as coregnum,
        cast(vatnum as varchar(255)) as vatnum,
        cast(importvatnum as varchar(255)) as importvatnum,
        null as upsnum,
        null as tax1099regnum,
        null as namecontrol,
        null as tcc,
        key_ as key_,
        null as dvrid,
        null as intrastatcode,
        null as girocontract,
        null as girocontractaccount,
        null as branchid,
        null as vatnumbranchid,
        null as importvatnumbranchid,
        null as activitycode,
        conversiondate as conversiondate,
        null as addrformat,
        null as companyregcomfr,
        null as packmaterialfeelicensenum,
        null as paymroutingdnb,
        null as paymtradernumber,
        null as issuingsignature,
        null as siacode,
        null as bankcentralbankpurposecode,
        null as bankcentralbankpurposetext,
        null as dba,
        foreignentityindicator as foreignentityindicator,
        combinedfedstatefiler as combinedfedstatefiler,
        lastfilingindicator as lastfilingindicator,
        validate_1099_onentry as validate1099onentry,
        null as legalformfr,
        null as shippingcalendarid,
        null as enterprisenumber,
        null as branchnumber,
        null as customscustomernumber_fi,
        null as customslicensenumber_fi,
        dataarea as dataarea,
        planningcompany as planningcompany,
        null as taxrepresentative,
        null as orgid,
        null as bankacctusedfor1099,
        payminstruction_1 as payminstruction1,
        payminstruction_2 as payminstruction2,
        payminstruction_3 as payminstruction3,
        payminstruction_4 as payminstruction4,
        null as legalrepresentativename_mx,
        null as legalrepresentativerfc_mx,
        null as legalrepresentativecurp_mx,
        null as ficreditorid_dk,
        isconsolidationcompany as isconsolidationcompany,
        iseliminationcompany as iseliminationcompany,
        null as accountingpersonnel_jp,
        null as templatefolder_w,
        null as companyrepresentative_jp,
        null as cnae_br,
        resident_w as resident_w,
        null as pfregnum_ru,
        null as raliencorpcountry,
        null as raliencorpname,
        null as rfullname,
        null as enterprisecode,
        null as commercialregistersection,
        null as commercialregisterinsetnumber,
        null as commercialregister,
        null as fss_ru,
        organizationlegalform_ru as organizationlegalform_ru,
        null as subordinatecode,
        null as fssaccount_ru,
        null as accountant_lt,
        null as accountofficerefnum,
        businesscommenceddate_jp as businesscommenceddate_jp,
        businessinitialcapital_jp as businessinitialcapital_jp,
        null as businessitem_jp,
        null as certifiedtaxaccountant_jp,
        null as cuc_it,
        null as head_lt,
        null as legalrepresentative_jp,
        null as personincharge_jp,
        printcorrinvoicelabel_de as printcorrinvoicelabel_de,
        null as printcorrinvlabeleffdate_de,
        printenterpriseregister_no as printenterpriseregister_no,
        printinnkppinaddress_ru as printinnkppinaddress_ru,
        null as taxauthority_ru,
        eeenablepersonaldatareadlog as eeenablepersonaldatareadlog,
        organizationtype as organizationtype,
        omoperatingunittype as omoperatingunittype,
        hcmworker as hcmworker,
        omoperatingunitnumber as omoperatingunitnumber,
        null as initials,
        null as childrennames,
        maritalstatus as maritalstatus,
        null as hobbies,
        gender as gender,
        namesequence as namesequence,
        null as phoneticfirstname,
        null as phoneticmiddlename,
        null as phoneticlastname,
        personaltitle as personaltitle,
        personalsuffix as personalsuffix,
        null as professionaltitle,
        null as professionalsuffix,
        birthmonth as birthmonth,
        birthday as birthday,
        birthyear as birthyear,
        anniversarymonth as anniversarymonth,
        anniversaryday as anniversaryday,
        anniversaryyear as anniversaryyear,
        communicatorsignin as communicatorsignin,
        numberofemployees as numberofemployees,
        orgnumber as orgnumber,
        abc as abc,
        teammembershipcriterion as teammembershipcriterion,
        description as description,
        isactive as isactive,
        null as teamadministrator,
        phoneticname as phoneticname,
        dunsnumberrecid as dunsnumberrecid,
        name as name,
        language_id as languageid,
        name_alias as namealias,
        party_number as partynumber,
        instance_relation_type as instancerelationtype,
        known_as as knownas,
        primary_address_location as primaryaddresslocation,
        primary_contact_email as primarycontactemail,
        primary_contact_fax as primarycontactfax,
        primary_contact_phone as primarycontactphone,
        primary_contact_telex as primarycontacttelex,
        primary_contact_url as primarycontacturl,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        createddatetime as createddatetime,
        createdby as createdby,
        recversion as recversion,
        relationtype as relationtype,
        partition as partition,
        recid as recid,
        eeenablerolechangelog as eeenablerolechangelog,
        null as biseanbarcodesetupid,
        null as biseancodeid,
        null as taxregimecode_mx
    from d365_source

)

select *
from renamed