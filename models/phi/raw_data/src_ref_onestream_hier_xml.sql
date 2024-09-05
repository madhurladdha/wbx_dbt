{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        transient=false,
        tags=["onestream", "rdm", "xml"],
        schema=env_var("DBT_SRC_RAW_DATA_SCHEMA"),
        on_schema_change="sync_all_columns"    
    )
}}
with dummy_cte as (
    select 1 as foo
)

select
cast(	null as  VARIANT ) xmldata
from dummy_cte
where 1 = 0