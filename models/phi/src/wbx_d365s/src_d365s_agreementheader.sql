with d365_source as (

    select *
    from {{ source("D365S", "agreementheader") }}
    where _fivetran_deleted = 'FALSE'
),

child_source as (

    select *
    from {{ source("D365S", "purchagreementheader") }}
    where _fivetran_deleted = 'FALSE'
    --filter removed as dataareaid is populated with generic 'dat' value
    --and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (
    select
        'D365S' as source,
        child_source.purchnumbersequence as purchnumbersequence,
        child_source.vendaccount as vendaccount,
        upper(child_source.vendordataareaid) as vendordataareaid,
        child_source.buyinglegalentity as buyinglegalentity,
        child_source.workflowstatus_psn as workflowstatus_psn,
        null as interestbasedonceb_psn,
        null as maximumamount_psn,
        null as minimumamount_psn,
        null as parentpurchagreementid_psn,
        null as procurementclassification_psn,
        null as purchagreementtype_psn,
        null as purpose_psn,
        null as renewable_psn,
        null as salesnumbersequence,
        null as custaccount,
        null as customerdataareaid,
        null as sellinglegalentity,
        d365_source.instancerelationtype as instancerelationtype,
        d365_source.agreementclassification as agreementclassification,
        d365_source.agreementstate as agreementstate,
        null as documenttitle,
        null as documentexternalreference,
        d365_source.currency as currency,
        d365_source.defaultagreementlinetype as defaultagreementlinetype,
        null as defaultagreementlineeffdate,
        null as defaultagreementlineexpdate,
        d365_source.originator as originator,
        d365_source.language as language,
        cast(d365_source.earliestlineeffectivedate as TIMESTAMP_NTZ)
            as earliestlineeffectivedate,
        cast(d365_source.latestlineexpirationdate as TIMESTAMP_NTZ)
            as latestlineexpirationdate,
        d365_source.isdeleted as isdeleted,
        d365_source.defaultdimension as defaultdimension,
        cast(d365_source.modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        d365_source.modifiedby as modifiedby,
        cast(d365_source.createddatetime as TIMESTAMP_NTZ) as createddatetime,
        d365_source.recversion as recversion,
        null as relationtype,
        d365_source.partition as partition,
        d365_source.recid as recid,
        null as agreementorigdepartment,
        null as agreementorigdepartmentdataareaid
    from d365_source
    left join child_source on d365_source.recid = child_source.recid


)

select * from renamed
