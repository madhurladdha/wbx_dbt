with d365_source as (
    select *
    from {{ source("D365", "agreement_classification") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        agreement_relation_type as agreementrelationtype,
        is_immutable as isimmutable,
        name as name,
        subcontractor_psn as subcontractor_psn,
        certification_psn as certification_psn,
        activity_psn as activity_psn,
        null as description_psn,
        direct_invoice_enable_psn as directinvoiceenable_psn,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed

