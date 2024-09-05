with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Scenario_Types') }}

),

renamed as (

    select
SCEN_TYPE_IDX,
SCEN_TYPE_CODE,
SCEN_TYPE_NAME,
SCEN_TYPE_DESCRIPTION,
SORTORDER_IDX

    from source

)

select * from renamed