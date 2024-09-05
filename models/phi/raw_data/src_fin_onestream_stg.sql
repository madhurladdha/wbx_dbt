{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        transient=false,
        tags=["onestream", "rdm"],
        schema=env_var("DBT_SRC_RAW_DATA_SCHEMA"),
        on_schema_change="sync_all_columns"    
    )
}}
with dummy_cte as (
    select 1 as foo
)

select
cast(	null	as 	varchar(255)	) as workflow_profile,
cast(	null	as 	varchar(255)	) as sourceid,
cast(	null	as 	varchar(255)	) as source_desc,
cast(	null	as 	varchar(255)	) as time,
cast(	null	as 	varchar(255)	) as scenario,
cast(	null	as 	varchar(255)	) as view,
cast(	null	as 	varchar(255)	) as source_entity,
cast(	null	as 	varchar(255)	) as target_entity,
cast(	null	as 	varchar(255)	) as source_account,
cast(	null	as 	varchar(255)	) as target_account,
cast(	null	as 	varchar(255)	) as source_flow,
cast(	null	as 	varchar(255)	) as target_flow,
cast(	null	as 	varchar(255)	) as origin,
cast(	null	as 	varchar(255)	) as source_ic,
cast(	null	as 	varchar(255)	) as target_ic,
cast(	null	as 	varchar(255)	) as source_mainud1,
cast(	null	as 	varchar(255)	) as target_mainud1,
cast(	null	as 	varchar(255)	) as source_mainud2,
cast(	null	as 	varchar(255)	) as target_mainud2,
cast(	null	as 	varchar(255)	) as source_mainud3,
cast(	null	as 	varchar(255)	) as target_mainud3,
cast(	null	as 	NUMBER(38,10)	) as amount
from dummy_cte
where 1 = 0