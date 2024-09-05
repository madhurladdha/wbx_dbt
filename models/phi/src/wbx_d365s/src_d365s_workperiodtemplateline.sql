/* This D365S src model has been modified as the underlying source replication table does not yet exist.
    The source table doesn't build if there is no source system data.
    For now, this is still pulling from the D365 source but filtering with the condition of 0=1 so that no data is passed.
    This needs to have the following udpates once the D365S table itself exists:
        -Change the source to source('D365S', 'workperiodtemplateline')
        -Remove the 0=1 filter
        -Update the field mapping according to the D365S field names.
*/

with source as (

    select * from {{ source('D365', 'work_period_template_line') }}
    where 0=1

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
