-- Output can have more than one source_item_identifier for one product_class_code, so removing source_item_identifier from comparision
{{ config(enabled=false, severity="warn") }}


{% set a_relation = ref("conv_sls_wtx_item_pushdown_xref") %}

{% set b_relation = ref("xref_wbx_sls_pushdown_item") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=["item_guid","source_item_identifier"],
        summarize=false,
    )
}}
