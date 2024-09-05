    {{
    config(
    materialized = 'view',
    )
}}

WITH old_dim AS 
        (
            SELECT * FROM {{source('EI_RDM','sls_scenario_dim')}} where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
        ),
        
converted_dim AS
(
    SELECT  
{{ dbt_utils.surrogate_key(['source_system','scenario_id']) }} as scenario_guid,
scenario_guid as scenario_guid_old,
SOURCE_SYSTEM,
SCENARIO_ID,
SCENARIO_CODE,
SCENARIO_DESC,
SCENARIO_TYPE_ID,
SCENARIO_TYPE_CODE,
SCENARIO_TYPE_NAME,
SCENARIO_STATUS_ID,
SCENARIO_STATUS_CODE,
SCENARIO_STATUS_DESC,
SCENARIO_STATUS_COLOUR,
SCENARIO_STATUS_DESCRIPTION,
LOAD_DATE,
UPDATE_DATE

FROM old_dim
)

select {{ dbt_utils.surrogate_key(['scenario_guid']) }} as unique_key,* from converted_dim