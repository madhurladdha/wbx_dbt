-- depends_on: {{ ref('stg_d_wtx_lkp_snapshot_date') }}
--changed from src_sls_wtx_lkp_snapshot_date to stg_d_wtx_lkp_snapshot_date


{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    unique_key = 'snapshot_date',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete+insert',
pre_hook=
        """
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        DELETE FROM {{ this }} WHERE SNAPSHOT_DATE=(select SNAP.snapshot_Date from {{ref('stg_d_wtx_lkp_snapshot_date')}} SNAP) 
        {% endif %}  
        """
    )
}}    


with STG_PROMO as
(
    select * from {{ ref('stg_d_wbx_promo') }}
),

HIST_PROMO as (
    select * from {{ ref('conv_sls_wtx_promo_dim') }}
),
guid_generation as
(
SELECT  
promo_guid,
null as promo_guid_old,
SOURCE_SYSTEM,
PROMO_ID,
PROMO_CODE,
PROMO_DESC,
PROMO_GROUP_ID,
PROMO_GROUP_DESC,
PROMO_CAT_ID,
PROMO_CAT_DESC,
PROMO_TACTIC_ID,
PROMO_TACTIC_DESC,
PROMO_SUB_TACTIC_ID,
PROMO_SUB_TACTIC_DESC,
PROMO_SUB_TACTIC_DISCOUNT,
PROMO_SUB_TACTIC_ISACTIVE,
PROMO_STAT_ID,
PROMO_STAT_DESC,
PROMO_PHASE_ID,
PROMO_PHASE_DESC,
PROMO_PHASE_LENGTH,
PROMO_PHASE_EFFECT_ID,
PROMO_PHASE_EFFECT_DESC,
PROMO_PHASE_TYPE_ID,
PROMO_PHASE_TYPE_DESC,
PROMO_PHASE_TYPE_UNIT,
AUTHORIZED_USER_NAME,
PERFORMANCE_START_DT,
PERFORMANCE_END_DT,
SHIP_START_DT,
SHIP_END_DT,
ALLOWANCE_START_DT,
ALLOWANCE_END_DT,
LAST_UPDATE_DATE,
BUY_IN_START_DT,
BUY_IN_END_DT,
IN_STORE_START_DT,
IN_STORE_END_DT,
TEMPLATE_START_DT,
TEMPLATE_END_DT,
FEATURE,
FEATURE_DESC,
PROMO_MECHANIC_NAME,
SNAPSHOT_DATE ,
UPDATE_DATE
from STG_PROMO
),

gen_unique_key as
(
  select  {{ dbt_utils.surrogate_key(['promo_guid','snapshot_date']) }} AS UNIQUE_KEY, * from guid_generation
),

new_dim as ( 
SELECT  

cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(a.promo_id as number(38,0) ) as promo_id  ,
cast(a.promo_guid as text(255) ) as promo_guid  ,
cast(b.promo_guid_old as text(255) ) as promo_guid_old  ,
cast(substring(a.promo_code,1,255) as text(255) ) as promo_code  ,
cast(substring(a.promo_desc,1,255) as text(255) ) as promo_desc  ,
cast(a.promo_group_id as number(38,0) ) as promo_group_id  ,
cast(substring(a.promo_group_desc,1,255) as text(255) ) as promo_group_desc  ,
cast(a.promo_cat_id as number(38,0) ) as promo_cat_id  ,
cast(substring(a.promo_cat_desc,1,255) as text(255) ) as promo_cat_desc  ,
cast(a.promo_tactic_id as number(38,0) ) as promo_tactic_id  ,
cast(substring(a.promo_tactic_desc,1,255) as text(255) ) as promo_tactic_desc  ,
cast(a.promo_sub_tactic_id as number(38,0) ) as promo_sub_tactic_id  ,
cast(substring(a.promo_sub_tactic_desc,1,255) as text(255) ) as promo_sub_tactic_desc  ,
cast(a.promo_sub_tactic_discount as number(38,0) ) as promo_sub_tactic_discount  ,
cast(a.promo_sub_tactic_isactive as number(38,0) ) as promo_sub_tactic_isactive  ,
cast(a.promo_stat_id as number(38,0) ) as promo_stat_id  ,
cast(substring(a.promo_stat_desc,1,255) as text(255) ) as promo_stat_desc  ,
cast(a.promo_phase_id as number(38,0) ) as promo_phase_id  ,
cast(substring(a.promo_phase_desc,1,255) as text(255) ) as promo_phase_desc  ,
cast(substring(a.promo_phase_length,1,255) as text(255) ) as promo_phase_length  ,
cast(a.promo_phase_effect_id as number(38,0) ) as promo_phase_effect_id  ,
cast(substring(a.promo_phase_effect_desc,1,255) as text(255) ) as promo_phase_effect_desc  ,
cast(a.promo_phase_type_id as number(38,0) ) as promo_phase_type_id  ,
cast(substring(a.promo_phase_type_desc,1,255) as text(255) ) as promo_phase_type_desc  ,
cast(substring(a.promo_phase_type_unit,1,255) as text(255) ) as promo_phase_type_unit  ,
cast(substring(a.authorized_user_name,1,255) as text(255) ) as authorized_user_name  ,
cast(a.performance_start_dt as timestamp_ntz(9) ) as performance_start_dt  ,
cast(a.performance_end_dt as timestamp_ntz(9) ) as performance_end_dt  ,
cast(a.ship_start_dt as timestamp_ntz(9) ) as ship_start_dt  ,
cast(a.ship_end_dt as timestamp_ntz(9) ) as ship_end_dt  ,
cast(a.allowance_start_dt as timestamp_ntz(9) ) as allowance_start_dt  ,
cast(a.allowance_end_dt as timestamp_ntz(9) ) as allowance_end_dt  ,
cast(a.last_update_date as timestamp_ntz(9) ) as last_update_date  ,
cast(a.buy_in_start_dt as timestamp_ntz(9) ) as buy_in_start_dt  ,
cast(a.buy_in_end_dt as timestamp_ntz(9) ) as buy_in_end_dt  ,
cast(a.in_store_start_dt as timestamp_ntz(9) ) as in_store_start_dt  ,
cast(a.in_store_end_dt as timestamp_ntz(9) ) as in_store_end_dt  ,
cast(a.template_start_dt as timestamp_ntz(9) ) as template_start_dt  ,
cast(a.template_end_dt as timestamp_ntz(9) ) as template_end_dt  ,
cast(a.snapshot_date as date) as snapshot_date  ,
cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
cast(substring(a.feature,1,50) as text(50) ) as feature  ,
cast(substring(a.feature_desc,1,200) as text(200) ) as feature_desc  ,
cast(substring(a.promo_mechanic_name,1,255) as text(255) ) as promo_mechanic_name  ,
cast(a.unique_key as text(255) ) as unique_key

FROM GEN_UNIQUE_KEY A 
LEFT JOIN HIST_PROMO B 
ON A.UNIQUE_KEY=B.UNIQUE_KEY

), 

old_dim as
(
SELECT
cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
    cast(a.promo_id as number(38,0) ) as promo_id  ,
    cast(a.promo_guid as text(255) ) as promo_guid  ,
    cast(a.promo_guid_old as text(255) ) as promo_guid_old  ,
    cast(substring(a.promo_code,1,255) as text(255) ) as promo_code  ,
    cast(substring(a.promo_desc,1,255) as text(255) ) as promo_desc  ,
    cast(a.promo_group_id as number(38,0) ) as promo_group_id  ,
    cast(substring(a.promo_group_desc,1,255) as text(255) ) as promo_group_desc  ,
    cast(a.promo_cat_id as number(38,0) ) as promo_cat_id  ,
    cast(substring(a.promo_cat_desc,1,255) as text(255) ) as promo_cat_desc  ,
    cast(a.promo_tactic_id as number(38,0) ) as promo_tactic_id  ,
    cast(substring(a.promo_tactic_desc,1,255) as text(255) ) as promo_tactic_desc  ,
    cast(a.promo_sub_tactic_id as number(38,0) ) as promo_sub_tactic_id  ,
    cast(substring(a.promo_sub_tactic_desc,1,255) as text(255) ) as promo_sub_tactic_desc  ,
    cast(a.promo_sub_tactic_discount as number(38,0) ) as promo_sub_tactic_discount  ,
    cast(a.promo_sub_tactic_isactive as number(38,0) ) as promo_sub_tactic_isactive  ,
    cast(a.promo_stat_id as number(38,0) ) as promo_stat_id  ,
    cast(substring(a.promo_stat_desc,1,255) as text(255) ) as promo_stat_desc  ,
    cast(a.promo_phase_id as number(38,0) ) as promo_phase_id  ,
    cast(substring(a.promo_phase_desc,1,255) as text(255) ) as promo_phase_desc  ,
    cast(substring(a.promo_phase_length,1,255) as varchar(255) ) as promo_phase_length  ,
    cast(a.promo_phase_effect_id as number(38,0) ) as promo_phase_effect_id  ,
    cast(substring(a.promo_phase_effect_desc,1,255) as text(255) ) as promo_phase_effect_desc  ,
    cast(a.promo_phase_type_id as number(38,0) ) as promo_phase_type_id  ,
    cast(substring(a.promo_phase_type_desc,1,255) as text(255) ) as promo_phase_type_desc  ,
    cast(substring(a.promo_phase_type_unit,1,255) as text(255) ) as promo_phase_type_unit  ,
    cast(substring(a.authorized_user_name,1,255) as text(255) ) as authorized_user_name  ,
    cast(a.performance_start_dt as timestamp_ntz(9) ) as performance_start_dt  ,
    cast(a.performance_end_dt as timestamp_ntz(9) ) as performance_end_dt  ,
    cast(a.ship_start_dt as timestamp_ntz(9) ) as ship_start_dt  ,
    cast(a.ship_end_dt as timestamp_ntz(9) ) as ship_end_dt  ,
    cast(a.allowance_start_dt as timestamp_ntz(9) ) as allowance_start_dt  ,
    cast(a.allowance_end_dt as timestamp_ntz(9) ) as allowance_end_dt  ,
    cast(a.last_update_date as timestamp_ntz(9) ) as last_update_date  ,
    cast(a.buy_in_start_dt as timestamp_ntz(9) ) as buy_in_start_dt  ,
    cast(a.buy_in_end_dt as timestamp_ntz(9) ) as buy_in_end_dt  ,
    cast(a.in_store_start_dt as timestamp_ntz(9) ) as in_store_start_dt  ,
    cast(a.in_store_end_dt as timestamp_ntz(9) ) as in_store_end_dt  ,
    cast(a.template_start_dt as timestamp_ntz(9) ) as template_start_dt  ,
    cast(a.template_end_dt as timestamp_ntz(9) ) as template_end_dt  ,
    cast(a.snapshot_date as date) as snapshot_date  ,
    cast(a.update_date as timestamp_ntz(9) ) as update_date  ,
    cast(substring(a.feature,1,50) as text(50) ) as feature  ,
    cast(substring(a.feature_desc,1,200) as text(200) ) as feature_desc  ,
    cast(substring(a.promo_mechanic_name,1,255) as text(255) ) as promo_mechanic_name  ,
    cast(a.unique_key as text(255) ) as unique_key

FROM HIST_PROMO A 
LEFT JOIN GEN_UNIQUE_KEY B 
ON A.UNIQUE_KEY=B.UNIQUE_KEY
WHERE B.unique_key is NULL
), 


Final_Dim 
as 
(SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 

Select * from Final_Dim 