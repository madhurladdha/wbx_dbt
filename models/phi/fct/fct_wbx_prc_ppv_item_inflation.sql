{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    tags = ["ppv","procurement","item_inflation","ppv_inflation", "inflation"],
    on_schema_change='sync_all_columns',
    full_refresh=false,
    pre_hook= """

      {%- set target_relation = adapter.get_relation(

      database=this.database,

      schema=this.schema,

      identifier=this.name) -%}

        {%- set table_exists=target_relation is not none -%}

        {%- if table_exists -%}

            delete from {{ this }} where version_dt = (select date_trunc('month', current_date)) AND  upper(Scenario)='LIVE'

        {%- endif -%}

        """ 
    )
}}

with old_table as
(
    select * from {{ref('conv_wbx_prc_ppv_item_inflation')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),
base_dim  as (
    select * from {{ ref ('stg_f_wbx_prc_ppv_item_inflation') }}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
old_model_conv as (
    select
        
        cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

        cast(substring(buyer_code_description,1,60) as text(60) ) as buyer_code_description  ,

        cast(substring(inflation_year,1,60) as text(60) ) as inflation_year  ,

        cast(version_dt as date) as version_dt  ,

        cast(substring(scenario,1,60) as text(60) ) as scenario  ,

        cast(calendar_date as date) as calendar_date  ,

        cast(inflation as number(28,9) ) as inflation  ,

        cast(load_date as timestamp_ntz(9) ) as load_date  
    from old_table
),
base_dim_conv as (
    Select
        cast(substring(buyer_code,1,60) as text(60) ) as buyer_code  ,

        cast(substring(buyer_code_description,1,60) as text(60) ) as buyer_code_description  ,

        cast(substring(inflation_year,1,60) as text(60) ) as inflation_year  ,

        cast(version_dt as date) as version_dt  ,

        cast(substring(scenario,1,60) as text(60) ) as scenario  ,

        cast(calendar_date as date) as calendar_date  ,

        cast(inflation as number(28,9) ) as inflation  ,

        cast(load_date as timestamp_ntz(9) ) as load_date  
    from base_dim 
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
    where inflation_year||scenario 
        not in (
                select distinct inflation_year||scenario from {{ this }}
                where scenario='BUDGET'
            ) 
    {% endif %}
)
select * from old_model_conv
union
select * from base_dim_conv