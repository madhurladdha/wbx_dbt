    {{
    config(
    materialized = 'view',
    )
}}

WITH old_dim AS 
        (
            SELECT * FROM {{source('EI_RDM','fin_onestream_month_Fact')}} where WORKFLOW_PROFILE like 'Weetabix%' and {{env_var("DBT_PICK_FROM_CONV")}}='Y'
        ),

converted_dim AS
(
    select
{{ dbt_utils.surrogate_key(['WORKFLOW_PROFILE','SOURCEID','SOURCE_ENTITY','TARGET_ENTITY','SOURCE_ACCOUNT','TARGET_ACCOUNT','SOURCE_IC','TARGET_IC','SOURCE_MAINUD1','TARGET_MAINUD1','SOURCE_MAINUD2','TARGET_MAINUD2','SOURCE_MAINUD3','TARGET_MAINUD3','FISCAL_PERIOD']) }} AS UNIQUE_KEY,
'{{env_var("DBT_SOURCE_SYSTEM")}}' AS source_system,
WORKFLOW_PROFILE,
SOURCEID,
SOURCE_DESC,
FISCAL_PERIOD,
SOURCE_ENTITY,
TARGET_ENTITY,
SOURCE_ACCOUNT,
TARGET_ACCOUNT,
SOURCE_IC,
TARGET_IC,
SOURCE_MAINUD1,
TARGET_MAINUD1,
SOURCE_MAINUD2,
TARGET_MAINUD2,
SOURCE_MAINUD3,
TARGET_MAINUD3,
MONTHLY_AMOUNT,
LOAD_DATE,
UPDATE_DATE
from old_dim
)

select * from converted_dim