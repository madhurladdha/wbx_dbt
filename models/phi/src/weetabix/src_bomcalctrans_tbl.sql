{{ config(materialized=env_var("DBT_MAT_TABLE"),tags=["sales","budget"]) }}
--change this source materialization to table. Since this is being used in the view v_sls_wtx_budget_pcos_projections
--as recursive cte. Keeping it as view was throwing SQL execution internal error: Processing aborted due to error 300010:423728544; incident 9425260.
select *
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'S' %}    
    from {{ ref("src_d365s_bomcalctrans_tbl") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'D' %}
    from {{ ref("src_d365_bomcalctrans_tbl") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'A' %}
    from {{ ref("src_ax_bomcalctrans_tbl") }}
{% endif %}