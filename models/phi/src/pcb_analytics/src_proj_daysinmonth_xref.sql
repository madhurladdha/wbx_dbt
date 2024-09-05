

with source as (

    select * from {{ source('PCB_ANALYTICS', 'proj_daysinmonth_xref') }}

),

renamed as (

    select
        date_id,
        daysleft,
        mondays_left,
        tuesdays_left,
        wednesdays_left,
        thursdays_left,
        fridays_left,
        saturdays_left,
        sundays_left,
        weekday_name,
        holidays_left

    from source

)

select * from renamed

