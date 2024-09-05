{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        unique_key="UNIQUE_KEY",
        transient=false,
        on_schema_change="sync_all_columns",
        tags="wbx_rdm",
    )
}}

with
    stg as (select * from {{ ref("src_wbx_item_categorization") }}),

    final as (

        select
            {{ dbt_utils.surrogate_key(["item_code", "company"]) }} as unique_key,
            cast(item_code as varchar2(60)) as source_item_identifier,
            cast(
                substr(item_id, regexp_instr(item_id, '-', 1, 1) + 1) as varchar2(255)
            ) as description,
            cast(company as varchar2(60)) as company_code,
            'NOT DEFINED' as new_item_buyer_group,
            'NOT DEFINED' as new_buyer_name,
            null as eur_flag,
            cast(null as varchar2(03)) approx_month_covered,
            current_timestamp as load_date
        from stg
        where item_code is not null

    )

select *
from final
