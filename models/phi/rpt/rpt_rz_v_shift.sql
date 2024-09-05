{{  config(      materialized=env_var("DBT_RZ_DS_MAT"),
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["redzone","OEE", 'v_run'],    )}}

with
    shift as (select * from {{ ref("src_rz_v_shift") }}),

    cp as (
        select
            to_boolean(ifnull(normalized_value, false)) as choke_point,
            source_value,
            source_site
        from {{ ref("src_rz_wbx_normalization") }}
        where reference_field = 'OEE_INCLUDE'
    )

select shift.*, ifnull(choke_point, false) as choke_point
from shift
left join
    cp on shift."locationName" = cp.source_value and shift."siteName" = cp.source_site
