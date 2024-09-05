{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tag=['ax_hist_dim']
    )
}}

WITH old_dim AS 
(
SELECT * FROM {{source('WBX_PROD','dim_wbx_project')}} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),

converted_dim AS
(
SELECT
unique_key,
project_guid,
project_guid_old,
SOURCE_SYSTEM,
PROJECT_ID,
DESCRIPTION,
PROJECT_STATUS,
PROJECT_STATUS_DESCR,
PROJECT_GROUP,
PROJECT_TYPE,
PROJECT_TYPE_DESCR,
SITE,
DEPARTMENT,
COST_CENTER,
PLANT,
CUSTOMER,
PRODUCT,
CAF_NO,
SORTINGID,
SORTINGID2_,
SORTINGID3_,
CREATION_DATE,
START_DATE_PROJECTED,
START_DATE_ACTUAL,
END_DATE_PROJECTED,
END_DATE_ACTUAL,
EXTENSION_DATE,
SOURCE_UPDATE_DATE,
project_controller_id,
PROJECT_CONTROLLER,
PROJECT_MANAGER_ID,
PROJECT_MANAGER,
SALES_MANAGER_ID,
SALES_MANAGER,
LOAD_DATE,
UPDATE_DATE,
SORTINGID_DESCR,
SORTINGID2_DESCR,
SORTINGID3_DESCR
FROM old_dim
)

select * from converted_dim