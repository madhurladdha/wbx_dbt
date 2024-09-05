{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns')
}}

with STG_PROJECT as
(
    select * from {{ ref('stg_d_wbx_proj') }}
),

HIST_PROJECT as (
    select * from {{ ref('conv_dim_wbx_project') }}
),

key_generation as (
    select {{ dbt_utils.surrogate_key(['PROJECT_GUID'])}} as UNIQUE_KEY,* from STG_PROJECT
),

new_dim as ( 
SELECT  
cast(a.unique_key as text(255))  AS UNIQUE_KEY,
cast(a.project_guid as text(255) ) as project_guid  ,
cast(b.project_guid_old as text(255) ) as project_guid_old ,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(substring(a.project_id,1,255) as text(255) ) as project_id  ,
cast(substring(a.description,1,255) as text(255) ) as description  ,
cast(substring(a.project_status,1,255) as text(255) ) as project_status  ,
cast(substring(a.project_status_descr,1,255) as text(255) ) as project_status_descr  ,
cast(substring(a.project_group,1,255) as text(255) ) as project_group  ,
cast(substring(a.project_type,1,255) as text(255) ) as project_type  ,
cast(substring(a.project_type_descr,1,255) as text(255) ) as project_type_descr  ,
cast(substring(a.site,1,255) as text(255) ) as site  ,
cast(substring(a.department,1,255) as text(255) ) as department  ,
cast(substring(a.cost_center,1,255) as text(255) ) as cost_center  ,
cast(substring(a.plant,1,255) as text(255) ) as plant  ,
cast(substring(a.customer,1,255) as text(255) ) as customer  ,
cast(substring(a.product,1,255) as text(255) ) as product  ,
cast(substring(a.caf_no,1,255) as text(255) ) as caf_no  ,
cast(substring(a.sortingid,1,255) as text(255) ) as sortingid  ,
cast(substring(a.sortingid2_,1,255) as text(255) ) as sortingid2_  ,
cast(substring(a.sortingid3_,1,255) as text(255) ) as sortingid3_  ,
cast(a.creation_date as timestamp_ntz(9) ) as creation_date  ,
cast(a.start_date_projected as timestamp_ntz(9) ) as start_date_projected  ,
cast(a.start_date_actual as timestamp_ntz(9) ) as start_date_actual  ,
cast(a.end_date_projected as timestamp_ntz(9) ) as end_date_projected  ,
cast(a.end_date_actual as timestamp_ntz(9) ) as end_date_actual  ,
cast(a.extension_date as timestamp_ntz(9) ) as extension_date  ,
cast(a.source_update_date as timestamp_ntz(9) ) as source_update_date  ,
cast(substring(a.project_controller_id,1,255) as text(255) ) as project_controller_id  ,
cast(substring(a.project_controller,1,255) as text(255) ) as project_controller  ,
cast(substring(a.project_manager_id,1,255) as text(255) ) as project_manager_id  ,
cast(substring(a.project_manager,1,255) as text(255) ) as project_manager  ,
cast(substring(a.sales_manager_id,1,255) as text(255) ) as sales_manager_id  ,
cast(substring(a.sales_manager,1,255) as text(255) ) as sales_manager  ,
cast(a.load_date as timestamp_ntz(9) ) as load_date  ,
cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
cast(substring(a.sortingid_descr,1,255) as text(255) ) as sortingid_descr  ,
cast(substring(a.sortingid2_descr,1,255) as text(255) ) as sortingid2_descr  ,
cast(substring(a.sortingid3_descr,1,255) as text(255) ) as sortingid3_descr


FROM KEY_GENERATION A 
LEFT JOIN HIST_PROJECT B 
ON A.UNIQUE_KEY=B.UNIQUE_KEY

), 

old_dim as
(
select
cast(a.unique_key as text(255))  AS UNIQUE_KEY,
cast(a.project_guid as text(255) ) as project_guid  ,
cast(a.project_guid_old as text(255) ) as project_guid_old ,
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(substring(a.project_id,1,255) as text(255) ) as project_id  ,
cast(substring(a.description,1,255) as text(255) ) as description  ,
cast(substring(a.project_status,1,255) as text(255) ) as project_status  ,
cast(substring(a.project_status_descr,1,255) as text(255) ) as project_status_descr  ,
cast(substring(a.project_group,1,255) as text(255) ) as project_group  ,
cast(substring(a.project_type,1,255) as text(255) ) as project_type  ,
cast(substring(a.project_type_descr,1,255) as text(255) ) as project_type_descr  ,
cast(substring(a.site,1,255) as text(255) ) as site  ,
cast(substring(a.department,1,255) as text(255) ) as department  ,
cast(substring(a.cost_center,1,255) as text(255) ) as cost_center  ,
cast(substring(a.plant,1,255) as text(255) ) as plant  ,
cast(substring(a.customer,1,255) as text(255) ) as customer  ,
cast(substring(a.product,1,255) as text(255) ) as product  ,
cast(substring(a.caf_no,1,255) as text(255) ) as caf_no  ,
cast(substring(a.sortingid,1,255) as text(255) ) as sortingid  ,
cast(substring(a.sortingid2_,1,255) as text(255) ) as sortingid2_  ,
cast(substring(a.sortingid3_,1,255) as text(255) ) as sortingid3_  ,
cast(a.creation_date as timestamp_ntz(9) ) as creation_date  ,
cast(a.start_date_projected as timestamp_ntz(9) ) as start_date_projected  ,
cast(a.start_date_actual as timestamp_ntz(9) ) as start_date_actual  ,
cast(a.end_date_projected as timestamp_ntz(9) ) as end_date_projected  ,
cast(a.end_date_actual as timestamp_ntz(9) ) as end_date_actual  ,
cast(a.extension_date as timestamp_ntz(9) ) as extension_date  ,
cast(a.source_update_date as timestamp_ntz(9) ) as source_update_date  ,
cast(substring(a.project_controller_id,1,255) as text(255) ) as project_controller_id  ,
cast(substring(a.project_controller,1,255) as text(255) ) as project_controller  ,
cast(substring(a.project_manager_id,1,255) as text(255) ) as project_manager_id  ,
cast(substring(a.project_manager,1,255) as text(255) ) as project_manager  ,
cast(substring(a.sales_manager_id,1,255) as text(255) ) as sales_manager_id  ,
cast(substring(a.sales_manager,1,255) as text(255) ) as sales_manager  ,
cast(a.load_date as timestamp_ntz(9) ) as load_date  ,
cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
cast(substring(a.sortingid_descr,1,255) as text(255) ) as sortingid_descr  ,
cast(substring(a.sortingid2_descr,1,255) as text(255) ) as sortingid2_descr  ,
cast(substring(a.sortingid3_descr,1,255) as text(255) ) as sortingid3_descr
FROM HIST_PROJECT A 
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
