with source as (

    select * from {{ source('D365', 'work_period_template_line') }}

),

renamed as (

    select
        period_template as periodtemplate,
        line_number as linenumber,
        period,
        number_of_periods as numberofperiods,
        explode_periods as explodeperiods,
        period_description as perioddescription,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
