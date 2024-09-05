{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('EI_RDM','proj_master_budg_line')}}  where  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),


converted_dim AS
(
    SELECT  
PROJECT_GUID as PROJECT_GUID_OLD,
{{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','PROJECT_ID']) }} as PROJECT_GUID,
SOURCE_SYSTEM,
PROJECT_ID,
trunc(PROJECT_TRANS_TYPE) as PROJECT_TRANS_TYPE,
PROJECT_TRANS_DESCR,
PROJECT_BUDG_CATEGORY,
ORIGINAL_BUDGET,
COMMITTED_REVISIONS,
UNCOMMITTED_REVISIONS,
TOTAL_BUDGET 
from old_dim
)

Select  {{ dbt_utils.surrogate_key(['PROJECT_GUID']) }} as UNIQUE_KEY,* from converted_dim

