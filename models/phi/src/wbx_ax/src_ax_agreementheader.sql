with source as (

    select * from {{ source('WEETABIX', 'agreementheader') }}

),

renamed as (

    select
        purchnumbersequence,
        vendaccount,
        vendordataareaid,
        buyinglegalentity,
        workflowstatus_psn,
        interestbasedonceb_psn,
        maximumamount_psn,
        minimumamount_psn,
        parentpurchagreementid_psn,
        procurementclassification_psn,
        purchagreementtype_psn,
        purpose_psn,
        renewable_psn,
        salesnumbersequence,
        custaccount,
        customerdataareaid,
        sellinglegalentity,
        instancerelationtype,
        agreementclassification,
        agreementstate,
        documenttitle,
        documentexternalreference,
        currency,
        defaultagreementlinetype,
        defaultagreementlineeffdate,
        defaultagreementlineexpdate,
        originator,
        language,
        earliestlineeffectivedate,
        latestlineexpirationdate,
        isdeleted,
        defaultdimension,
        modifieddatetime,
        modifiedby,
        createddatetime,
        recversion,
        relationtype,
        partition,
        recid,
        agreementorigdepartment,
        agreementorigdepartmentdataareaid

    from source

)

select * from renamed
