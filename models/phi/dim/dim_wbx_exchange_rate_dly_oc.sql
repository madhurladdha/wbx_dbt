{{
    config(
    on_schema_change='sync_all_columns',
    tags = "rdm_core"
    )
}}


with src as (
    select * from {{ref('stg_d_wbx_exchange_rate')}}
),

curr_dim as (
    select * from {{source('EI_RDM','currency_exch_rate_dly_dim')}}  
),

hist as
(
    select * from {{ref('conv_currency_exch_rate_dly_dim')}} where 1=2
),

A as (
    SELECT CURR_FROM_CODE, 
    CURR_TO_CODE, 
    CURR_CONV_RT,
     CURR_CONV_RT_I as CURR_CONV_RATE_I, 
     EFF_D_ID, 
     EFF_FROM_D, 
     EXPIR_D_ID, 
     EXPIR_TO_D, 
     ROLLFWD_ROW, 
     '{{env_var("DBT_SOURCE_SYSTEM")}}' as SOURCE_SYSTEM 
    from src
),

B as(
    SELECT 
    CURR_TO_CODE as CURR_FROM_CODE,
    CURR_FROM_CODE as CURR_TO_CODE, 
    CURR_CONV_RT_I as CURR_CONV_RT,
    CURR_CONV_RT as CURR_CONV_RATE_I,
    EFF_D_ID, 
     EFF_FROM_D, 
     EXPIR_D_ID, 
     EXPIR_TO_D, 
     ROLLFWD_ROW, 
     '{{env_var("DBT_SOURCE_SYSTEM")}}' as SOURCE_SYSTEM
    from src
),

c as(
    SELECT CURR_FROM_CODE,
      CURR_TO_CODE, 
    CURR_CONV_RT,
     CURR_CONV_RATE_I ,
     EFF_D_ID, 
     EFF_FROM_D, 
     EXPIR_D_ID, 
     EXPIR_TO_D, 
     ROLLFWD_ROW, 
     '{{env_var("DBT_SOURCE_SYSTEM")}}' as SOURCE_SYSTEM
     from curr_dim where CURR_FROM_CODE='EUR' and CURR_TO_CODE='USD'
),

d as(
    SELECT 
    CURR_TO_CODE as CURR_FROM_CODE, 
    CURR_FROM_CODE as CURR_TO_CODE, 
    CURR_CONV_RATE_I as CURR_CONV_RT,
    CURR_CONV_RT as CURR_CONV_RATE_I,
    EFF_D_ID, 
    EFF_FROM_D, 
    EXPIR_D_ID, 
    EXPIR_TO_D, 
    ROLLFWD_ROW, 
    '{{env_var("DBT_SOURCE_SYSTEM")}}' as SOURCE_SYSTEM
      from curr_dim where CURR_FROM_CODE='EUR' and CURR_TO_CODE='USD'
),

Final as(
    select * from A
    union
     select * from b
    union
     select * from c
    union
     select * from d
    
)


select distinct * from final