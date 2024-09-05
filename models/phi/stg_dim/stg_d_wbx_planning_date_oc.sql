with Source as 
(
select * from {{ ref('src_wbix_tbldimdate') }}
),

stg as(
select '{{ env_var("DBT_SOURCE_SYSTEM") }}'  as  SOURCE_SYSTEM
,DD_PLANYEARWW AS PLANNING_WEEK_CODE 
,DD_DATETIME AS CALENDAR_DATE
,DECODE(UPPER(DD_DDD),'MON','Monday','TUE','Tuesday','WED','Wednesday','THU','Thursday','FRI','Friday','SAT','Saturday','SUN','Sunday') AS CALENDAR_DAY_OF_WEEK
,DATEADD(dd, -364, DD_DATETIME) as LY_DATE
,DATEADD(dd, -364*2 , DD_DATETIME) as PY_DATE
,SUBSTR(DD_PLANYEARWW,5,6) as PLANNING_WEEK_NO
,DD_PLANYEARWW AS PLANNING_YEAR_WEEK_NO
,MIN(DD_DATETIME) OVER (PARTITION BY DD_PLANYEARWW) AS PLANNING_WEEK_START_DT
,MAX(DD_DATETIME) OVER (PARTITION BY DD_PLANYEARWW) AS PLANNING_WEEK_END_DT
,COUNT(0) OVER (PARTITION BY DD_PLANYEARWW) AS PLANNING_WEEK_NUMOFDAYS
,DD_PLANPERIODWW AS PLANNING_MONTH_WEEK
,NULL AS PLANNING_WEEK_DESC
,NULL AS PLANNING_WEEK_DESCRLONG
,NULL AS PLANNING_SUBWEEK_CODE
,NULL AS SUBWEEK_FLAG
,NULL AS PLANNING_SUBWEEK_START_DT
,NULL AS PLANNING_SUBWEEK_END_DT
,NULL AS PLANNING_SUBWEEK_DESC
,DD_PLANPERIOD AS PLANNING_MONTH_NO
,DD_PLANYEAR * 100 + DD_PLANPERIOD AS PLANNING_MONTH_CODE
,MIN(DD_DATETIME) OVER (PARTITION BY DD_PLANYEAR * 100 + DD_PLANPERIOD ) AS PLANNING_MONTH_START_DT
,MAX(DD_DATETIME) OVER (PARTITION BY DD_PLANYEAR * 100 + DD_PLANPERIOD ) AS PLANNING_MONTH_END_DT
,COUNT(0) OVER (PARTITION BY DD_PLANYEAR * 100 + DD_PLANPERIOD ) AS PLANNING_MONTH_NUMOFDAYS
,DD_PLANQTRINT - (DD_PLANYEAR * 100 ) AS PLANNING_QUARTER_NO
,DD_PLANQTRINT AS PLANNING_QUARTER_CODE
,MIN(DD_DATETIME) OVER ( PARTITION BY DD_PLANQTRINT) AS PLANNING_QUARTER_START_DT
,MAX(DD_DATETIME) OVER ( PARTITION BY DD_PLANQTRINT) AS PLANNING_QUARTER_END_DT
,COUNT(0) OVER ( PARTITION BY DD_PLANQTRINT) AS PLANNING_QUARTER_NUMOFDAYS
,DD_PLANYEAR AS PLANNING_YEAR_NO
,MIN(DD_DATETIME) OVER ( PARTITION BY DD_PLANYEAR) AS PLANNING_YEAR_START_DT
,MAX(DD_DATETIME) OVER ( PARTITION BY DD_PLANYEAR) AS PLANNING_YEAR_END_DT
,COUNT(DD_DATETIME) OVER ( PARTITION BY DD_PLANYEAR) AS PLANNING_YEAR_NUMOFDAYS
from Source 

)

select 
SOURCE_SYSTEM
,PLANNING_WEEK_CODE
,CALENDAR_DATE
,CALENDAR_DAY_OF_WEEK
,LY_DATE
,PY_DATE
,PLANNING_WEEK_NO
,PLANNING_YEAR_WEEK_NO
,PLANNING_WEEK_START_DT
,PLANNING_WEEK_END_DT
,PLANNING_WEEK_NUMOFDAYS
,PLANNING_MONTH_WEEK
,PLANNING_WEEK_DESC
,PLANNING_WEEK_DESCRLONG
,PLANNING_SUBWEEK_CODE
,SUBWEEK_FLAG
,PLANNING_SUBWEEK_START_DT
,PLANNING_SUBWEEK_END_DT
,PLANNING_SUBWEEK_DESC
,PLANNING_MONTH_NO
,PLANNING_MONTH_CODE
,PLANNING_MONTH_START_DT
,PLANNING_MONTH_END_DT
,PLANNING_MONTH_NUMOFDAYS
,PLANNING_QUARTER_NO
,PLANNING_QUARTER_CODE
,PLANNING_QUARTER_START_DT
,PLANNING_QUARTER_END_DT
,PLANNING_QUARTER_NUMOFDAYS
,PLANNING_YEAR_NO
,PLANNING_YEAR_START_DT
,PLANNING_YEAR_END_DT
,PLANNING_YEAR_NUMOFDAYS

from stg