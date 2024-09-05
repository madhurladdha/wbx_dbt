with SCEN as
(
    select * from {{ref('src_exc_dim_scenario')}}
),

STAT as
(
SELECT 
SCEN_STATUS_IDX,
SCEN_STATUS_CODE,
SCEN_STATUS_NAME,
SCEN_STATUS_COLOUR,
SCEN_STATUS_DESCRIPTION
from {{ref('src_exc_dim_scenario_status')}} 
),

SCEN_TYP as
(
SELECT SCEN_TYPE_IDX,
SCEN_TYPE_NAME,
SCEN_TYPE_CODE 
FROM {{ref('src_exc_dim_scenario_types')}}
),

stg as (
SELECT  
'{{env_var("DBT_SOURCE_SYSTEM")}}' as SOURCE_SYSTEM
,SCEN.SCEN_IDX AS SCENARIO_ID
,SCEN.SCEN_CODE AS SCENARIO_CODE
,SCEN.SCEN_NAME AS SCENARIO_DESC 
,SCEN_TYP.SCEN_TYPE_IDX AS SCENARIO_TYPE_ID
,SCEN_TYP.SCEN_TYPE_CODE AS SCENARIO_TYPE_CODE
,SCEN_TYP.SCEN_TYPE_NAME AS SCENARIO_TYPE_NAME
,STAT.SCEN_STATUS_IDX AS SCENARIO_STATUS_ID
,STAT.SCEN_STATUS_CODE AS SCENARIO_STATUS_CODE
,STAT.SCEN_STATUS_NAME AS SCENARIO_STATUS_DESC
,STAT.SCEN_STATUS_COLOUR AS SCENARIO_STATUS_COLOUR
,STAT.SCEN_STATUS_DESCRIPTION AS SCENARIO_STATUS_DESCRIPTION
from SCEN
LEFT JOIN
STAT ON SCEN.SCEN_STATUS_IDX=STAT.SCEN_STATUS_IDX
LEFT JOIN
SCEN_TYP on SCEN.SCEN_TYPE_IDX=SCEN_TYP.SCEN_TYPE_IDX
),

Final as
(
    SELECT
SOURCE_SYSTEM,
{{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SCENARIO_ID']) }} as SCENARIO_GUID,
SCENARIO_GUID as SCENARIO_GUID_OLD,
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
TRUNC(CURRENT_DATE,'DD') AS LOAD_DATE,
TRUNC(CURRENT_DATE,'DD')  AS UPDATE_DATE
FROM
STG
)

select * from Final