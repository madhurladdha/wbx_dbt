{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    on_schema_change='sync_all_columns'
    )
}}

with history as (

    select * from {{ source('R_EI_SYSADM','dim_date_oc') }} WHERE SOURCE_SYSTEM ='{{env_var("DBT_SOURCE_SYSTEM")}}'

),

converted_dim AS
(
    SELECT  
FISCAL_DATE_ID,
FISCAL_PERIOD_NO,
FISCAL_YEAR_PERIOD_NO,
FISCAL_PERIOD_DESC,
FISCAL_PERIOD_BEGIN_DT,
FISCAL_PERIOD_END_DT,
FISCAL_YEAR_QUARTER_NO,
FISCAL_QUARTER_DESC,
FISCAL_QUARTER_START_DT,
FISCAL_QUARTER_END_DT,
FISCAL_YEAR,
FISCAL_YEAR_BEGIN_DT,
FISCAL_YEAR_END_DT,
FISCAL_YEAR_WEEK_NO,
CALENDAR_DATE_ID,
CALENDAR_DATE,
CALENDAR_DAY_OF_WEEK,
CALENDAR_YEAR,
CALENDAR_YEAR_BEGIN_DT,
CALENDAR_YEAR_END_DT,
CALENDAR_YEAR_QUARTER_NO,
CALENDAR_QUARTER_DESC,
CALENDAR_QUARTER_START_DT,
CALENDAR_QUARTER_END_DT,
CALENDAR_YEAR_MONTH_NO,
CALENDAR_MONTH_NO,
CALENDAR_MONTH_NAME,
CALENDAR_MONTH_DESC,
CALENDAR_MONTH_START_DT,
CALENDAR_MONTH_END_DT,
CALENDAR_YEAR_WEEK_NO,
CALENDAR_WEEK_BEGIN_DT,
CALENDAR_WEEK_END_DT,
CALENDAR_BUSINESS_DAY_FLAG,
SOURCE_IND,
LOAD_DATE,
UPDATE_DATE,
REPORT_FISCAL_YEAR_PERIOD_NO,
REPORT_FISCAL_YEAR,
SOURCE_SYSTEM,
OC_PERIOD_NAME,
OC_FISCAL_YEAR,
OC_FISCAL_PERIOD_NO,
OC_FISCAL_YEAR_PERIOD_NO,
OC_FISCAL_PERIOD_DESC,
OC_FISCAL_PERIOD_BEGIN_DT,
OC_FISCAL_PERIOD_END_DT
from history
)

select * from converted_dim