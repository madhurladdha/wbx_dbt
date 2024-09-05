{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["manufacturing", "supply_Schedule", "wbx", "daily","inventory"],
        on_schema_change="sync_all_columns",
        unique_key="unique_key",
        incremental_strategy="merge",
        full_refresh=false,
        pre_hook="""
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
                delete from {{this}}
                where snapshot_date=to_date(convert_timezone('UTC',current_timestamp)) or
                snapshot_date <= to_date(convert_timezone('UTC',current_timestamp)) - 180
            {% endif %}  
            """,
    )
}}


with
    old_table as (
        select *
        from {{ ref("conv_inv_wtx_supply_sched_dly_fact") }}
        {% if check_table_exists(this.schema, this.table) == "False" %}
            limit {{ env_var("DBT_NO_LIMIT") }}  -- --------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if fct_wbx_mfg_supply_sched_dly is not present.
        {% else %} limit {{ env_var("DBT_LIMIT") }}  -- ---Variable DBT_LIMIT variable is set to 0 to load nothing if fct_wbx_mfg_supply_sched_dly table exists.

        {% endif %}
    ),

    incr_table as (
        select *
        from {{ ref("int_f_wbx_mfg_supply_sched_dly") }}
        qualify row_number() over(partition by unique_key order by 1)=1
        {% if check_table_exists(this.schema, this.table) == "True" %}
            limit {{ env_var("DBT_NO_LIMIT") }}
        {% else %} limit {{ env_var("DBT_LIMIT") }}
        {% endif %}
    )

select *
from incr_table
union
select *
from old_table
