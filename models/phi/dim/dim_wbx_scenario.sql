{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns')
}}

with STG_SCENARIO as
(
    select * from {{ ref('stg_d_wbx_scenario') }}
),

HIST_SCENARIO as (
    select * from {{ ref('conv_sls_scenario_dim') }}
),

key_generation as (
    select {{ dbt_utils.surrogate_key(['SCENARIO_GUID'])}} as UNIQUE_KEY,* from STG_SCENARIO
),

new_dim as ( 
SELECT  
cast(a.unique_key as text(255))  AS UNIQUE_KEY,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(b.scenario_guid_old as text(255) ) as scenario_guid_old,
cast(a.scenario_guid as text(255) ) as scenario_guid  ,
cast(a.scenario_id as number(38,0) ) as scenario_id  ,
cast(substring(a.scenario_code,1,255) as text(255) ) as scenario_code  ,
cast(substring(a.scenario_desc,1,255) as text(255) ) as scenario_desc  ,
cast(a.scenario_type_id as number(38,0) ) as scenario_type_id  ,
cast(substring(a.scenario_type_code,1,255) as text(255) ) as scenario_type_code  ,
cast(substring(a.scenario_type_name,1,255) as text(255) ) as scenario_type_name  ,
cast(a.scenario_status_id as number(38,0) ) as scenario_status_id  ,
cast(substring(a.scenario_status_code,1,255) as text(255) ) as scenario_status_code  ,
cast(substring(a.scenario_status_desc,1,255) as text(255) ) as scenario_status_desc  ,
cast(substring(a.scenario_status_colour,1,255) as text(255) ) as scenario_status_colour  ,
cast(substring(a.scenario_status_description,1,255) as text(255) ) as scenario_status_description  ,
cast(a.load_date as timestamp_ntz(9) ) as load_date  ,
cast(a.update_date as timestamp_ntz(9) ) as update_date

FROM KEY_GENERATION A 
LEFT JOIN HIST_SCENARIO B 
ON A.UNIQUE_KEY=B.UNIQUE_KEY

), 

old_dim as
(
SELECT
cast(a.unique_key as text(255) ) as unique_key,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(a.scenario_guid_old as text(255) ) as scenario_guid_old,
cast(a.scenario_guid as text(255) ) as scenario_guid  ,
cast(a.scenario_id as number(38,0) ) as scenario_id  ,
cast(substring(a.scenario_code,1,255) as text(255) ) as scenario_code  ,
cast(substring(a.scenario_desc,1,255) as text(255) ) as scenario_desc  ,
cast(a.scenario_type_id as number(38,0) ) as scenario_type_id  ,
cast(substring(a.scenario_type_code,1,255) as text(255) ) as scenario_type_code  ,
cast(substring(a.scenario_type_name,1,255) as text(255) ) as scenario_type_name  ,
cast(a.scenario_status_id as number(38,0) ) as scenario_status_id  ,
cast(substring(a.scenario_status_code,1,255) as text(255) ) as scenario_status_code  ,
cast(substring(a.scenario_status_desc,1,255) as text(255) ) as scenario_status_desc  ,
cast(substring(a.scenario_status_colour,1,255) as text(255) ) as scenario_status_colour  ,
cast(substring(a.scenario_status_description,1,255) as text(255) ) as scenario_status_description  ,
a.load_date ,
a.update_date  

FROM HIST_SCENARIO A 
LEFT JOIN KEY_GENERATION B 
ON A.UNIQUE_KEY=B.UNIQUE_KEY
WHERE B.unique_key is NULL
), 


Final_Dim 
as 
(SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 

Select * from Final_Dim 