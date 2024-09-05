    {{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

WITH old_dim AS 
    (
        SELECT * FROM {{source('WBX_PROD','dim_wbx_fin_onestream')}} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
    ),

converted_dim AS
(
select
VERSION_DATE,
SOURCE_SYSTEM,
WORKFLOW_PROFILE,
SOURCEID,
SOURCE_DESC,
TIME,
SCENARIO,
VIEW,
SOURCE_ENTITY,
TARGET_ENTITY,
SOURCE_ACCOUNT,
TARGET_ACCOUNT,
SOURCE_FLOW,
TARGET_FLOW,
ORIGIN,
SOURCE_IC,
TARGET_IC,
SOURCE_MAINUD1,
TARGET_MAINUD1,
SOURCE_MAINUD2,
TARGET_MAINUD2,
SOURCE_MAINUD3,
TARGET_MAINUD3,
AMOUNT,
LOAD_DATE,
FILE_NAME
from old_dim
)

select * from converted_dim