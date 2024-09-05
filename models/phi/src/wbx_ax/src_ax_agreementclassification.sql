

with source as (

    select * from {{ source('WEETABIX', 'agreementclassification') }}

),

renamed as (

    select
        agreementrelationtype,
        isimmutable,
        name,
        subcontractor_psn,
        certification_psn,
        activity_psn,
        description_psn,
        directinvoiceenable_psn,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
