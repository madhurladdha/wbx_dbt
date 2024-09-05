with d365_source as (

        select *
        from {{ source("D365", "agreement_header") }} where  _FIVETRAN_DELETED='FALSE'
    ),

    renamed as (
        select
            'D365' as source,
            purchnumbersequence as purchnumbersequence,
            vendaccount as vendaccount,
            upper(vendordataareaid) as vendordataareaid,
            buyinglegalentity as buyinglegalentity,
            workflowstatus_psn as workflowstatus_psn,
            null as interestbasedonceb_psn,
            null as maximumamount_psn,
            null as minimumamount_psn,
            null as parentpurchagreementid_psn,
            null as procurementclassification_psn,
            null as purchagreementtype_psn,
            null as purpose_psn,
            null as renewable_psn,
            salesnumbersequence as salesnumbersequence,
            custaccount as custaccount,
            upper(customerdataareaid) as customerdataareaid,
            sellinglegalentity as sellinglegalentity,
            instance_relation_type as instancerelationtype,
            agreement_classification as agreementclassification,
            agreement_state as agreementstate,
            null as documenttitle,
            null as documentexternalreference,
            currency as currency,
            default_agreement_line_type as defaultagreementlinetype,
            null as defaultagreementlineeffdate,
            null as defaultagreementlineexpdate,
            originator as originator,
            language as language,
            earliest_line_effective_date as earliestlineeffectivedate,
            latest_line_expiration_date as latestlineexpirationdate,
            is_deleted as isdeleted,
            default_dimension  as defaultdimension,
            modifieddatetime as modifieddatetime,
            modifiedby as modifiedby,
            createddatetime as createddatetime,
            recversion as recversion,
            relationtype as relationtype,
            partition as partition,
            recid as recid,
            null as agreementorigdepartment,
            null as agreementorigdepartmentdataareaid
        from d365_source where upper(vendordataareaid) in {{env_var("DBT_D365_COMPANY_FILTER")}}
    )

select * from renamed
