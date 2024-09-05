
{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}

WITH int as
(SELECT * FROM {{ref('int_d_wbx_fin_onestream_month')}}
),

old as 
(SELECT * FROM {{ref('conv_fin_onestream_month_fact')}}
),


new_dim as 
(
select SOURCE_SYSTEM,
UNIQUE_KEY,
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
UPDATE_DATE FROM int
),

old_dim as (
select old.SOURCE_SYSTEM,
old.UNIQUE_KEY,
old.WORKFLOW_PROFILE,
old.SOURCEID,
old.SOURCE_DESC,
old.FISCAL_PERIOD,
old.SOURCE_ENTITY,
old.TARGET_ENTITY,
old.SOURCE_ACCOUNT,
old.TARGET_ACCOUNT,
old.SOURCE_IC,
old.TARGET_IC,
old.SOURCE_MAINUD1,
old.TARGET_MAINUD1,
old.SOURCE_MAINUD2,
old.TARGET_MAINUD2,
old.SOURCE_MAINUD3,
old.TARGET_MAINUD3,
old.MONTHLY_AMOUNT,
old.LOAD_DATE,
old.UPDATE_DATE FROM old 
left join new_dim on old.UNIQUE_KEY=new_dim.UNIQUE_KEY
where new_dim.source_system is null
),

final as 
(
select * from new_dim 
union
select * from old_dim
)

select * from final