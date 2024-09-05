

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_ROB_Statuses') }}

),

renamed as (

    select
        status_idx,
        status_code,
        status_name,
        status_colour,
        status_description,
        status_sort,
        apptype_idx,
        verb,
        includeinlivescenario,
        includeinuserscenario,
        createeventforthisstatus,
        amendable,
        isusercreated,
        isenabled,
        attachfundatthisstatus,
        fundrequiredforthisstatus,
        includeinfundapproved,
        isstatusapprovedorabove,
        isstatusconfirmedorabove,
        includeinfunddrawn

    from source

)

select * from renamed
