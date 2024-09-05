with
d365_source as (
    select *
    from {{ source("D365", "hcm_position_worker_assignment") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        position as position,
        valid_from as validfrom,
        validfromtzid,
        valid_to as validto,
        validtotzid,
        assignment_reason_code as assignmentreasoncode,
        worker as worker,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
