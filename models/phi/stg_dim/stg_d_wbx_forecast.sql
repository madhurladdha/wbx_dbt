{{
    config(
    tags = ["ppv","procurement","forecast","ppv_forecast"]
         )
}}




with src as(
    select * from {{ref('src_wbx_forecast')}}
),

exp as(
select
RTrim(LTrim(substr(Item_ID,regexp_instr(Item_ID,'-',1,1)+1))) as DESCRIPTION,
item_code as source_item_identifier,
UPPER(COMPANY) as COMPANY_CODE,
UPPER(Scenario) as SCENARIO,
YEAR,
nvl(Replace(Replace(Oct_eur,'¬',''), ',', '') ,0) as OCT_COST_EUR,
nvl(Replace(Replace(Nov_eur,'¬',''), ',', '') ,0) as NOV_COST_EUR,
nvl(Replace(Replace(Dec_eur,'¬',''), ',', '') ,0) as DEC_COST_EUR,
nvl(Replace(Replace(Jan_eur,'¬',''), ',', '') ,0) as JAN_COST_EUR,
nvl(Replace(Replace(Feb_eur,'¬',''), ',', '') ,0) as FEB_COST_EUR,
nvl(Replace(Replace(Mar_eur,'¬',''), ',', '') ,0) as MAR_COST_EUR,
nvl(Replace(Replace(Apr_eur,'¬',''), ',', '') ,0) as APR_COST_EUR,
nvl(Replace(Replace(May_eur,'¬',''), ',', '') ,0) as MAY_COST_EUR,
nvl(Replace(Replace(Jun_eur,'¬',''), ',', '') ,0) as JUN_COST_EUR,
nvl(Replace(Replace(Jul_eur,'¬',''), ',', '') ,0) as JUL_COST_EUR,
nvl(Replace(Replace(Aug_eur,'¬',''), ',', '') ,0) as AUG_COST_EUR,
nvl(Replace(Replace(Sep_eur,'¬',''), ',', '') ,0) as SEP_COST_EUR,
nvl(Replace(Replace(Nov_pound,'£',''), ',', '') ,0) as NOV_COST_BASE,
nvl(Replace(Replace(Dec_pound,'£',''), ',', '') ,0) as DEC_COST_BASE,
nvl(Replace(Replace(Jan_pound,'£',''), ',', '') ,0) as JAN_COST_BASE,
nvl(Replace(Replace(Feb_pound,'£',''), ',', '') ,0) as FEB_COST_BASE,
nvl(Replace(Replace(Mar_pound,'£',''), ',', '') ,0) as MAR_COST_BASE,
nvl(Replace(Replace(Apr_pound,'£',''), ',', '') ,0) as APR_COST_BASE,
nvl(Replace(Replace(May_pound,'£',''), ',', '') ,0) as MAY_COST_BASE,
nvl(Replace(Replace(Jun_pound,'£',''), ',', '') ,0) as JUN_COST_BASE,
nvl(Replace(Replace(Jul_pound,'£',''), ',', '') ,0) as JUL_COST_BASE,
nvl(Replace(Replace(Aug_pound,'£',''), ',', '') ,0) as AUG_COST_BASE,
nvl(Replace(Replace(Sep_pound,'£',''), ',', '') ,0) as SEP_COST_BASE,
nvl(Replace(Replace(Oct_pound,'£',''), ',', '') ,0) as OCT_COST_BASE,
nvl(Replace(Oct, ',', ''),0) as OCT_VOLUME,
nvl(Replace(Nov, ',', ''),0) as NOV_VOLUME,
nvl(Replace(Dec, ',', ''),0) as DEC_VOLUME,
nvl(Replace(Jan, ',', ''),0) as JAN_VOLUME,
nvl(Replace(Feb, ',', ''),0) as FEB_VOLUME,
nvl(Replace(Mar, ',', ''),0) as MAR_VOLUME,
nvl(Replace(Apr, ',', ''),0) as APR_VOLUME,
nvl(Replace(May, ',', ''),0) as MAY_VOLUME,
nvl(Replace(Jun, ',', ''),0) as JUN_VOLUME,
nvl(Replace(Jul, ',', ''),0) as JUL_VOLUME,
nvl(Replace(Aug, ',', ''),0) as AUG_VOLUME,
nvl(Replace(Sep, ',', ''),0) as SEP_VOLUME

from src
)

select 
cast(substring(source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,
cast(substring(description,1,255) as text(255) ) as description  ,
cast(substring(company_code,1,60) as text(60) ) as company_code  ,
cast(substring(year,1,20) as text(20) ) as year  ,
cast(substring(scenario,1,60) as text(60) ) as scenario  ,
cast(oct_volume as number(38,10) ) as oct_volume  ,
cast(nov_volume as number(38,10) ) as nov_volume  ,
cast(dec_volume as number(38,10) ) as dec_volume  ,
cast(jan_volume as number(38,10) ) as jan_volume  ,
cast(feb_volume as number(38,10) ) as feb_volume  ,
cast(mar_volume as number(38,10) ) as mar_volume  ,
cast(apr_volume as number(38,10) ) as apr_volume  ,
cast(may_volume as number(38,10) ) as may_volume  ,
cast(jun_volume as number(38,10) ) as jun_volume  ,
cast(jul_volume as number(38,10) ) as jul_volume  ,
cast(aug_volume as number(38,10) ) as aug_volume  ,
cast(sep_volume as number(38,10) ) as sep_volume  ,
cast(oct_cost_eur as number(38,10) ) as oct_cost_eur  ,
cast(nov_cost_eur as number(38,10) ) as nov_cost_eur  ,
cast(dec_cost_eur as number(38,10) ) as dec_cost_eur  ,
cast(jan_cost_eur as number(38,10) ) as jan_cost_eur  ,
cast(feb_cost_eur as number(38,10) ) as feb_cost_eur  ,
cast(mar_cost_eur as number(38,10) ) as mar_cost_eur  ,
cast(apr_cost_eur as number(38,10) ) as apr_cost_eur  ,
cast(may_cost_eur as number(38,10) ) as may_cost_eur  ,
cast(jun_cost_eur as number(38,10) ) as jun_cost_eur  ,
cast(jul_cost_eur as number(38,10) ) as jul_cost_eur  ,
cast(aug_cost_eur as number(38,10) ) as aug_cost_eur  ,
cast(sep_cost_eur as number(38,10) ) as sep_cost_eur  ,
cast(oct_cost_base as number(38,10) ) as oct_cost_base  ,
cast(nov_cost_base as number(38,10) ) as nov_cost_base  ,
cast(dec_cost_base as number(38,10) ) as dec_cost_base  ,
cast(jan_cost_base as number(38,10) ) as jan_cost_base  ,
cast(feb_cost_base as number(38,10) ) as feb_cost_base  ,
cast(mar_cost_base as number(38,10) ) as mar_cost_base  ,
cast(apr_cost_base as number(38,10) ) as apr_cost_base  ,
cast(may_cost_base as number(38,10) ) as may_cost_base  ,
cast(jun_cost_base as number(38,10) ) as jun_cost_base  ,
cast(jul_cost_base as number(38,10) ) as jul_cost_base  ,
cast(aug_cost_base as number(38,10) ) as aug_cost_base  ,
cast(sep_cost_base as number(38,10) ) as sep_cost_base  ,
current_timestamp as load_date 
 from exp