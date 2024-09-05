{{
    config(
    on_schema_change='sync_all_columns'
    )
}}


with SRC as
(
    select * from {{ref('stg_d_wbx_budg')}}
),

HIST as
(
    select * from {{ref('conv_proj_master_budg_line') }} where 1=2  --as this is a truncate load table.using old model just for lineage and test purpose
),

STG as
(
select
 PROJECT_GUID
,SOURCE_SYSTEM
,PROJECT_ID
,PROJECT_TRANS_TYPE
,PROJECT_TRANS_DESCR
,PROJECT_BUDG_CATEGORY
,ORIGINAL_BUDGET
,COMMITTED_REVISIONS
,UNCOMMITTED_REVISIONS
,TOTAL_BUDGET
,ROW_NUMBER() OVER (PARTITION BY PROJECT_GUID,SOURCE_SYSTEM,PROJECT_ID,PROJECT_TRANS_TYPE,PROJECT_BUDG_CATEGORY  ORDER BY 1) rowNum
from src
),

FINAL as(
select
    cast({{ dbt_utils.surrogate_key(['PROJECT_GUID']) }} as text(255)) as UNIQUE_KEY,
    cast(project_guid as text(255) ) as project_guid  ,
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(project_id,1,255) as text(255) ) as project_id  ,
    cast(substring(project_trans_type,1,255) as text(255) ) as project_trans_type  ,
    cast(substring(project_trans_descr,1,255) as text(255) ) as project_trans_descr  ,
    cast(substring(project_budg_category,1,255) as text(255) ) as project_budg_category  ,
    cast(original_budget as number(38,10) ) as original_budget  ,
    cast(committed_revisions as number(38,10) ) as committed_revisions  ,
    cast(uncommitted_revisions as number(38,10) ) as uncommitted_revisions  ,
    cast(total_budget as number(38,10) ) as total_budget

From STG where rowNum=1
)

select * from Final