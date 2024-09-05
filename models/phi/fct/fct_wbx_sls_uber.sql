 {{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    transient=false,
    tags=["sales","uber"],
    snowflake_warehouse=env_var('DBT_WBX_SF_WH'),
    on_schema_change="sync_all_columns",
    pre_hook="""
            {{ truncate_if_exists(this.schema, this.table) }}
            """,
) 
}}
/* for Forecast and budget,there are 2 cte's for each in the union.one table is for wbx uk data and another is for iberica data */

with 
actual as 
(
    select * from {{ref('int_f_wbx_actual_uber')}}
),

ibe_forecast as 
(
    select * from {{ref('int_f_wbx_ibe_forecast_uber')}}  --this cte has forecast data  for  iberica
),

forecast as(
    select * from {{ref('int_f_wbx_forecast_uber')}}------this cte has forecast data  for  wbx uk
),

gl_pcos_ppv as
(
    select * from {{ref('int_f_wbx_gl_pcos_ppv_uber')}}      
),

gl_pcos_std_je as 
(
    select * from {{ref('int_f_wbx_gl_pcos_std_je_uber')}}  
),

gl_trade as 
(
    select * from {{ref('int_f_wbx_gl_trade_uber')}}  
),

ibe_budget as 
(
    select * from {{ref('int_f_wbx_ibe_budget_uber')}}---this cte has budget data  for  iberica
),

budget as(
    select * from {{ref('int_f_wbx_budget_uber')}}---this cte has budget data  for  wbx uk
),

gcam as 
(
    select * from {{ref('int_f_wbx_gl_gcam_uber')}}
),

gl_dni as 
(
    select * from {{ref('int_f_wbx_gl_dni_uber')}}
),

epos as(
    select * from {{ref('int_f_wbx_epos_uber')}}
),


/*excluded out terms cte,we don't want terms data in uber as we have dedicated view for terms and promo.but can be added if needed*/






final as 
(

select * from actual
union 
select * from gl_pcos_ppv 
union 
select * from gl_pcos_std_je 
union 
select * from gl_trade 
union 
select * from ibe_budget
union 
select * from budget
union
select * from ibe_forecast
union
select * from forecast
union
select * from gcam
union 
select * from gl_dni
union
select * from epos
/*union
select * from terms
*/ 
/*commenting out above union part,we don't want terms data in uber as we have dedicated view for terms and promo.*/
 
)

select * from final