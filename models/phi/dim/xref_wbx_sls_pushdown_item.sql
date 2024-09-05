{{ config(materialized=env_var("DBT_MAT_TABLE")) }}

with
    source as (select * from {{ source("EI_RDM", "sls_wtx_item_pushdown_xref") }}),

    wref_wbx_item_pushdown as (

        select
            source_system,
            product_class_code,
            source_item_identifier,
            {{
                dbt_utils.surrogate_key(
                    ["source.source_system", "source.source_item_identifier"]
                )
            }} as item_guid

        from source

    )

select
    cast(substring(source_system, 1, 10) as text(10)) as source_system,
    cast(substring(product_class_code, 1, 60) as text(60)) as product_class_code,
    cast(substring(source_item_identifier, 1, 60) as text(60) ) as source_item_identifier,
    cast(item_guid as text(255)) as item_guid

from wref_wbx_item_pushdown
