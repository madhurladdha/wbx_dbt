{{
    config(
        tags=["inventory", "item_cost","inv_item_cost"]
    )
}}
 
--added partion by to address issue in https://phi-it-enterprise-intelligence.atlassian.net/browse/WBXPRODSUP-460
with src as (
select * from {{ref('stg_d_wbx_inv_item_cost')}}
qualify ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM,SOURCE_ITEM_IDENTIFIER,SOURCE_BUSINESS_UNIT_CODE,SOURCE_LOCATION_CODE,SOURCE_LOT_CODE,SOURCE_COST_METHOD_CODE,EFF_DATE,VARIANT_CODE ORDER BY 1) = 1
),

eff_curr as
(
SELECT SOURCE_SYSTEM,
SOURCE_BUSINESS_UNIT_CODE ,
COMPANY_DEFAULT_CURRENCY_CODE ,
PARENT_CURRENCY_CODE ,
EFFECTIVE_DATE ,
EXPIRATION_DATE
FROM {{ref('src_ref_effective_currency_dim')}} WHERE SOURCE_SYSTEM ='WEETABIX' 
),



Max_date as (
Select max(EFF_FROM_D) as EFF_FROM_D 
FROM {{ref('src_currency_exch_rate_dly_dim')}}
),------generating max date to feed in excahnge rate macro, as in iics lkp,sql overide is used to get only record with max effective date


transform_1 as(
select 
--'0' as location_guid,
--'0' as lot_guid,
{{ dbt_utils.surrogate_key(['src.source_system','source_location_code','src.source_business_unit_code']) }} as location_guid,
{{ dbt_utils.surrogate_key(['src.source_system','src.source_business_unit_code','source_item_identifier','source_lot_code']) }} as lot_guid,
case when COST_METHOD_CODE_LKP.NORMALIZED_VALUE is null then SOURCE_COST_METHOD_CODE else COST_METHOD_CODE_LKP.NORMALIZED_VALUE end   as TARGET_COST_METHOD_CODE,
case when COST_METHOD_DESC_LKP.NORMALIZED_VALUE is null then SOURCE_COST_METHOD_CODE else COST_METHOD_DESC_LKP.NORMALIZED_VALUE end  as TARGET_COST_METHOD_DESC,
'USD' as PHI_CURRENCY,
'150365' as expir_d_id,
'1' as oc_base_conv_rt,
src.source_system,
SOURCE_ITEM_IDENTIFIER,
src.SOURCE_BUSINESS_UNIT_CODE,
SOURCE_LOCATION_CODE,
SOURCE_LOT_CODE,
SOURCE_COST_METHOD_CODE,
ITEM_UNIT_COST,
EFF_DATE,
MAX_date.EFF_FROM_D,
EXPIR_DATE,
SOURCE_UPDATED_DATETIME,
TRANSACTION_CURRENCY,
VARIANT_CODE,
eff_curr.COMPANY_DEFAULT_CURRENCY_CODE,
eff_curr.PARENT_CURRENCY_CODE,
{{ dbt_utils.surrogate_key(['src.SOURCE_SYSTEM','SOURCE_ITEM_IDENTIFIER']) }} AS ITEM_GUID,
{{ dbt_utils.surrogate_key(['src.SOURCE_SYSTEM','src.SOURCE_BUSINESS_UNIT_CODE',"'PLANT_DC'"]) }} AS BUSINESS_UNIT_ADDRESS_GUID
from src
inner join Max_date on 1=1
left join eff_curr on src.SOURCE_BUSINESS_UNIT_CODE=eff_curr.SOURCE_BUSINESS_UNIT_CODE and src.EFF_DATE>=eff_curr.effective_date and src.EFF_DATE<=eff_curr.EXPIRATION_DATE
LEFT JOIN {{ ent_dbt_package.lkp_normalization('SRC.SOURCE_SYSTEM','ITEM','COST_METHOD_CODE','UPPER(SRC.SOURCE_COST_METHOD_CODE)','COST_METHOD_CODE_LKP') }} 
LEFT JOIN {{ ent_dbt_package.lkp_normalization('SRC.SOURCE_SYSTEM','ITEM','COST_METHOD_DESC','UPPER(SRC.SOURCE_COST_METHOD_CODE)','COST_METHOD_DESC_LKP') }}
),


currency_conversion as (
select 
location_guid,
lot_guid,
TARGET_COST_METHOD_CODE,
TARGET_COST_METHOD_DESC,
PHI_CURRENCY,
expir_d_id,
oc_base_conv_rt,
transform_1.source_system,
SOURCE_ITEM_IDENTIFIER,
SOURCE_BUSINESS_UNIT_CODE,
SOURCE_LOCATION_CODE,
SOURCE_LOT_CODE,
SOURCE_COST_METHOD_CODE,
ITEM_UNIT_COST,
EFF_DATE,
transform_1.EFF_FROM_D,
EXPIR_DATE,
SOURCE_UPDATED_DATETIME,
TRANSACTION_CURRENCY,
VARIANT_CODE,
COMPANY_DEFAULT_CURRENCY_CODE,
PARENT_CURRENCY_CODE,
ITEM_GUID,
BUSINESS_UNIT_ADDRESS_GUID,
trans.CURR_CONV_RT as TRANS_CURR_CONV_RT,
pcomp.CURR_CONV_RT as pcomp_CURR_CONV_RT,
corp.CURR_CONV_RT as corp_CURR_CONV_RT
from transform_1
left join {{ lkp_exchange_rate_daily('COMPANY_DEFAULT_CURRENCY_CODE','TRANSACTION_CURRENCY','transform_1.EFF_FROM_D','trans') }}
left join {{ lkp_exchange_rate_daily('COMPANY_DEFAULT_CURRENCY_CODE','PARENT_CURRENCY_CODE','transform_1.EFF_FROM_D','pcomp') }}
left join {{ lkp_exchange_rate_daily('COMPANY_DEFAULT_CURRENCY_CODE',"'USD'",'transform_1.EFF_FROM_D','corp') }}
),


Final as(

select location_guid,
lot_guid,
TARGET_COST_METHOD_CODE,
TARGET_COST_METHOD_DESC,
PHI_CURRENCY,
oc_base_conv_rt,
source_system,
SOURCE_ITEM_IDENTIFIER,
SOURCE_BUSINESS_UNIT_CODE,
SOURCE_LOCATION_CODE,
SOURCE_LOT_CODE,
SOURCE_COST_METHOD_CODE,
ITEM_UNIT_COST,
TRANSACTION_CURRENCY,
VARIANT_CODE,
COMPANY_DEFAULT_CURRENCY_CODE AS BASE_CURRENCY,
PARENT_CURRENCY_CODE as PCOMP_CURRENCY,
ITEM_GUID,
BUSINESS_UNIT_ADDRESS_GUID,
case when TRANSACTION_CURRENCY=COMPANY_DEFAULT_CURRENCY_CODE then 1 else TRANS_CURR_CONV_RT end as  v_oc_trans_conv_rt,
case when v_OC_TRANS_CONV_RT is null then 0 else v_OC_TRANS_CONV_RT end  as oc_trans_conv_rt,
case when PHI_CURRENCY=COMPANY_DEFAULT_CURRENCY_CODE then 1 else CORP_CURR_CONV_RT end as v_OC_CORP_CONV_RT,
case when v_OC_CORP_CONV_RT is null then 0 else v_OC_CORP_CONV_RT end as OC_CORP_CONV_RT,
case when PARENT_CURRENCY_CODE=COMPANY_DEFAULT_CURRENCY_CODE then 1 else  PCOMP_CURR_CONV_RT end as v_OC_PCOMP_CONV_RT,
case when v_OC_PCOMP_CONV_RT is null then 0 else v_OC_PCOMP_CONV_RT end as OC_PCOMP_CONV_RT,
case when v_OC_TRANS_CONV_RT=0 then 0 else ITEM_UNIT_COST * (1/v_OC_TRANS_CONV_RT) end  as v_OC_BASE_ITEM_UNIT_PRIM_COST,
case when v_OC_BASE_ITEM_UNIT_PRIM_COST is null then 0 else v_OC_BASE_ITEM_UNIT_PRIM_COST end  as OC_BASE_ITEM_UNIT_PRIM_COST,
case when (v_OC_BASE_ITEM_UNIT_PRIM_COST * v_OC_CORP_CONV_RT) is null then 0  else (v_OC_BASE_ITEM_UNIT_PRIM_COST * v_OC_CORP_CONV_RT)  end as OC_CORP_ITEM_UNIT_PRIM_COST,
case when (v_OC_BASE_ITEM_UNIT_PRIM_COST * v_OC_PCOMP_CONV_RT) is null then 0  else (v_OC_BASE_ITEM_UNIT_PRIM_COST * v_OC_PCOMP_CONV_RT) end as OC_PCOMP_ITEM_UNIT_PRIM_COST
 from currency_conversion
)


select  
    cast({{ dbt_utils.surrogate_key(['SOURCE_ITEM_IDENTIFIER','SOURCE_BUSINESS_UNIT_CODE','SOURCE_LOCATION_CODE','SOURCE_LOT_CODE','SOURCE_COST_METHOD_CODE','VARIANT_CODE']) }} as text(255) )as unique_key,
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,
    cast(item_guid as text(255) ) as item_guid  ,
    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,
    cast(substring(source_location_code,1,255) as text(255) ) as source_location_code  ,
    cast(location_guid as text(255) ) as location_guid  ,
    cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,
    cast(lot_guid as text(255) ) as lot_guid  ,
    cast(substring(source_cost_method_code,1,255) as text(255) ) as source_cost_method_code  ,
    cast(substring(target_cost_method_code,1,255) as text(255) ) as target_cost_method_code  ,
    cast(substring(target_cost_method_desc,1,255) as text(255) ) as target_cost_method_desc  ,
    cast(item_unit_cost as number(38,10) ) as item_unit_cost  ,
    cast(substring(transaction_currency,1,255) as text(255) ) as transaction_currency  ,
    cast(substring(base_currency,1,20) as text(20) ) as base_currency  ,
    cast(substring(phi_currency,1,20) as text(20) ) as phi_currency  ,
    cast(substring(pcomp_currency,1,20) as text(20) ) as pcomp_currency  ,
    cast(oc_trans_conv_rt as number(38,10) ) as oc_trans_conv_rt  ,
    cast(oc_base_conv_rt as number(38,10) ) as oc_base_conv_rt  ,
    cast(oc_corp_conv_rt as number(38,10) ) as oc_corp_conv_rt  ,
    cast(oc_pcomp_conv_rt as number(38,10) ) as oc_pcomp_conv_rt  ,
    cast(oc_base_item_unit_prim_cost as number(38,10) ) as oc_base_item_unit_prim_cost  ,
    cast(oc_corp_item_unit_prim_cost as number(38,10) ) as oc_corp_item_unit_prim_cost  ,
    cast(oc_pcomp_item_unit_prim_cost as number(38,10) ) as oc_pcomp_item_unit_prim_cost  ,
    cast(substring(variant_code,1,255) as text(255) ) as variant_code
from final 
