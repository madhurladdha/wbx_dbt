with
d365_source as (
    select *
    from {{ source("D365S", "dirpartytable") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        null as businessactivity_sa,
        null as businessactivitydesc_sa,
        null as filenumber_sa,
        null as companynafcode,
        null as businessnumber_ca,
        null as softwareidentificationcode_ca,
        null as fiscalcode_it,
        null as companytype_mx,
        null as rfc_mx,
        null as curp_mx,
        null as stateinscription_mx,
        null as legalnature_it,
        null as bank,
        null as giro,
        null as regnum,
        null as coregnum,
        null as vatnum,
        null as importvatnum,
        null as upsnum,
        null as tax1099regnum,
        null as namecontrol,
        null as tcc,
        null as key_,
        null as dvrid,
        null as intrastatcode,
        null as girocontract,
        null as girocontractaccount,
        null as branchid,
        null as vatnumbranchid,
        null as importvatnumbranchid,
        null as activitycode,
        null as conversiondate,
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
        null as foreignentityindicator,
        null as combinedfedstatefiler,
        null as lastfilingindicator,
        null as validate1099onentry,
        null as legalformfr,
        null as shippingcalendarid,
        null as enterprisenumber,
        null as branchnumber,
        null as customscustomernumber_fi,
        null as customslicensenumber_fi,
        null as dataarea,
        null as planningcompany,
        null as taxrepresentative,
        null as orgid,
        null as bankacctusedfor1099,
        null as payminstruction1,
        null as payminstruction2,
        null as payminstruction3,
        null as payminstruction4,
        null as legalrepresentativename_mx,
        null as legalrepresentativerfc_mx,
        null as legalrepresentativecurp_mx,
        null as ficreditorid_dk,
        null as isconsolidationcompany,
        null as iseliminationcompany,
        null as accountingpersonnel_jp,
        null as templatefolder_w,
        null as companyrepresentative_jp,
        null as cnae_br,
        null as resident_w,
        null as pfregnum_ru,
        null as raliencorpcountry,
        null as raliencorpname,
        null as rfullname,
        null as enterprisecode,
        null as commercialregistersection,
        null as commercialregisterinsetnumber,
        null as commercialregister,
        null as fss_ru,
        null as organizationlegalform_ru,
        null as subordinatecode,
        null as fssaccount_ru,
        null as accountant_lt,
        null as accountofficerefnum,
        null as businesscommenceddate_jp,
        null as businessinitialcapital_jp,
        null as businessitem_jp,
        null as certifiedtaxaccountant_jp,
        null as cuc_it,
        null as head_lt,
        null as legalrepresentative_jp,
        null as personincharge_jp,
        null as printcorrinvoicelabel_de,
        null as printcorrinvlabeleffdate_de,
        null as printenterpriseregister_no,
        null as printinnkppinaddress_ru,
        null as taxauthority_ru,
        null as eeenablepersonaldatareadlog,
        null as organizationtype,
        null as omoperatingunittype,
        null as hcmworker,
        null as omoperatingunitnumber,
        null as initials,
        null as childrennames,
        null as maritalstatus,
        null as hobbies,
        null as gender,
        null as namesequence,
        null as phoneticfirstname,
        null as phoneticmiddlename,
        null as phoneticlastname,
        null as personaltitle,
        null as personalsuffix,
        null as professionaltitle,
        null as professionalsuffix,
        null as birthmonth,
        null as birthday,
        null as birthyear,
        null as anniversarymonth,
        null as anniversaryday,
        null as anniversaryyear,
        null as communicatorsignin,
        null as numberofemployees,
        null as orgnumber,
        null as abc,
        null as teammembershipcriterion,
        null as description,
        null as isactive,
        null as teamadministrator,
        null as phoneticname,
        null as dunsnumberrecid,
        name as name,
        languageid as languageid,
        namealias as namealias,
        partynumber as partynumber,
        null as instancerelationtype,
        knownas as knownas,
        primaryaddresslocation as primaryaddresslocation,
        primarycontactemail as primarycontactemail,
        primarycontactfax as primarycontactfax,
        primarycontactphone as primarycontactphone,
        primarycontacttelex as primarycontacttelex,
        primarycontacturl as primarycontacturl,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        recversion as recversion,
        null as relationtype,
        partition as partition,
        recid as recid,
        null as eeenablerolechangelog,
        null as biseanbarcodesetupid,
        null as biseancodeid,
        null as taxregimecode_mx
    from d365_source

)

select *
from renamed