with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Scenario_Status') }}

),

renamed as (

    select
SCEN_STATUS_IDX,
SCEN_STATUS_CODE,
SCEN_STATUS_NAME,
SCEN_STATUS_COLOUR,
SCEN_STATUS_DESCRIPTION,
ISUSERSELECTABLE,
ISOPEN,
ISLISTABLE,
ISCLONABLE,
ISDELETEABLE,
ISEXPORTABLE,
ISCLOSABLE,
ISSAVEABLE,
ENABLEDCONTROLSVERB,
SORTORDER_IDX


    from source

)

select * from renamed