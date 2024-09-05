-- depends_on: {{ ref('dim_wbx_fc_snapshot') }}
-- depends_on: {{ ref('dim_wbx_promo') }}


{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    tags=["sales_promo_hist","sales_archive"],
    unique_key = 'snapshot_date',
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    full_refresh=false,
       post_hook=
        """
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        DELETE FROM {{ref('dim_wbx_promo')}} WHERE SNAPSHOT_DATE NOT IN (SELECT DISTINCT SNAPSHOT_DATE FROM {{ref('dim_wbx_fc_snapshot')}} )
        OR SNAPSHOT_DATE < CURRENT_DATE-372
        {% endif %}  
        """
    )
}} 

with hist_fct as
(
    select * from {{ source('FACTS_FOR_COMPARE', 'sls_wtx_promo_dim_hist') }} 
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),

base_fct  as (
    select * from {{ref('dim_wbx_promo')}}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),


old as (
      select  
        source_system,
        promo_id,
        {{ dbt_utils.surrogate_key(['source_system','promo_id']) }} as promo_guid,
        promo_guid as promo_guid_old,
        promo_code,
        promo_desc,
        promo_group_id,
        promo_group_desc,
        promo_cat_id,
        promo_cat_desc,
        promo_tactic_id,
        promo_tactic_desc,
        promo_sub_tactic_id,
        promo_sub_tactic_desc,
        promo_sub_tactic_discount,
        promo_sub_tactic_isactive,
        promo_stat_id,
        promo_stat_desc,
        promo_phase_id,
        promo_phase_desc,
        promo_phase_length,
        promo_phase_effect_id,
        promo_phase_effect_desc,
        promo_phase_type_id,
        promo_phase_type_desc,
        promo_phase_type_unit,
        authorized_user_name,
        performance_start_dt,
        performance_end_dt,
        ship_start_dt,
        ship_end_dt,
        allowance_start_dt,
        allowance_end_dt,
        last_update_date,
        buy_in_start_dt,
        buy_in_end_dt,
        in_store_start_dt,
        in_store_end_dt,
        template_start_dt,
        template_end_dt,
        snapshot_date,
        update_date,
        feature,
        feature_desc,
        promo_mechanic_name
        

    from hist_fct

),

old_with_unique as(
    select * ,{{ dbt_utils.surrogate_key(['promo_guid','snapshot_date']) }} AS UNIQUE_KEY from old
),

new as (
     {% if check_table_exists( this.schema, this.table ) == 'True' %}
    SELECT * FROM  base_fct WHERE SNAPSHOT_DATE > (SELECT MAX(SNAPSHOT_DATE) FROM {{this}})
    {% else %}
    select * from base_fct
    {% endif %}
),

Final as(
    select * from old_with_unique
    union
    select * from new
)


select * from final

