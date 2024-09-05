{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key='VERSION_DATE',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete+insert' ,
    
    pre_hook=
        """
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        DELETE FROM {{ this }} WHERE TRUNC(VERSION_DATE,'DAY')=TRUNC(CURRENT_DATE,'DAY')
		OR VERSION_DATE<=(SELECT DATEADD(DAY,-90,MAX(VERSION_DATE)) FROM  {{ this }})
        {% endif %}  
        """
    )
}}


with old as
(
     select * from {{ ref('conv_dim_wbx_fin_onestream') }} 
),

stg as 
(
    select * from {{ ref('stg_d_wbx_fin_onestream') }}
),


new_dim as
(
select cast(current_date as TIMESTAMP_NTZ) as VERSION_DATE,
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
from stg
),


--combining with old data 

old_dim as(
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
from old where version_date not in (select distinct version_date from new_dim)
),

final as
 (
     select * from new_dim
     union
     select * from old_dim
 )

 select * from final