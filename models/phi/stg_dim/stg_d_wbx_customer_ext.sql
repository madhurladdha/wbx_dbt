/*----------------------------------------------------------------------------------------------------------------------------------*/
/*	Mike Traub 18-Dec-2019  *********************************************************************************************************/
/* SQL used to populate the Customer Extension table for Weetabix.																	*/
/* All of these attributes are specific to the Weetabix Customer hierarchy and hierarchy attributes that are required for Sales.	*/
/* This will have to run after the Customer Master itself is loaded as the Customer master is one of the primary sources, combined with		*/
/*	the other main WEETABIX Customer tables.																					*/
/*	Jasmeet Singh 03- July -2020 : Join on WBXCUSTTABLEEXT to add 5 more Fields.
/*	Gopal 05-August-2020 : to add FIN_DIM_COST_CENTRE VARCHAR2(255),FIN_DIM_CUSTOMER VARCHAR2(255),FIN_DIM_DEPARTMENT VARCHAR2(255), */
/*FIN_DIM_SITE VARCHAR2(255) Fields.																					*/
/*  Mike Traub 20-Aug-20:                                                                                                       */
/*  Adding a second UNION part to get the inactive customers from MDS.  Can't do a direct join to the customer master to get these.	*/
/*----------------------------------------------------------------------------------------------------------------------------------*/



with CM as 
(
select * from {{ref('dim_wbx_customer')}}

),

CT as(
select * from {{ref('src_custtable')}}
),


DAVS as(
Select 
*
from {{ref('src_dimensionattributevaluesetitem')}}
),

DAV as
(
select * from {{ref('src_dimensionattributevalue')}}
),


DA as
(
select * from {{ref('src_dimensionattribute')}}
),

cc as 
(
select 
DAVS.PARTITION,
DAVS.DIMENSIONATTRIBUTEVALUESET,
DAVS.DISPLAYVALUE,
DA.NAME 
 from DAVS
INNER JOIN DAV ON DAVS.PARTITION = DAV.PARTITION AND DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
INNER JOIN DA ON DAV.PARTITION = DA.PARTITION AND DAV.DIMENSIONATTRIBUTE = DA.RECID
WHERE DA.NAME in('CostCenters')
),

FC as
(
Select 
DAVS.PARTITION,
DAVS.DIMENSIONATTRIBUTEVALUESET,
DAVS.DISPLAYVALUE,
DA.NAME 
 from DAVS  
INNER JOIN DAV ON DAVS.PARTITION = DAV.PARTITION AND DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
INNER JOIN DA ON DAV.PARTITION = DA.PARTITION AND DAV.DIMENSIONATTRIBUTE = DA.RECID
WHERE DA.NAME in('Customer')
),

DP as
(
Select 
DAVS.PARTITION,
DAVS.DIMENSIONATTRIBUTEVALUESET,
DAVS.DISPLAYVALUE,
DA.NAME 
from DAVS
INNER JOIN DAV ON DAVS.PARTITION = DAV.PARTITION AND DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
INNER JOIN DA ON DAV.PARTITION = DA.PARTITION AND DAV.DIMENSIONATTRIBUTE = DA.RECID
  WHERE DA.NAME in('Department')
),
  
si as
(
Select 
DAVS.PARTITION,
DAVS.DIMENSIONATTRIBUTEVALUESET,
DAVS.DISPLAYVALUE,
DA.NAME 
from DAVS
INNER JOIN DAV ON DAVS.PARTITION = DAV.PARTITION AND DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
INNER JOIN DA ON DAV.PARTITION = DA.PARTITION AND DAV.DIMENSIONATTRIBUTE = DA.RECID
WHERE DA.NAME in('Sites')
),
market as
(
select * from {{ref('src_wbxcust_market')}}
),
  
submarket as
(
select * from {{ref('src_wbxcust_submarket')}}
),
  
tradeclass as
(
select * from {{ref('src_wbxcust_trade_class')}}
),

tradegroup as
(
select * from {{ref('src_wbxcust_trade_group')}} 
),

tradetype as
(
select * from {{ref('src_wbxcust_trade_type')}}
),


tradesector as(
select * from {{ref('src_wbxcust_trade_sector')}}
),

set1 as(
/* From this union data for D365 will flow in.The SEQ number is hardcode to 0 as when discussed with steve we dont have it in D365 and business is ok*/
SELECT 
distinct 
CM.SOURCE_SYSTEM AS SOURCE_SYSTEM,
CM.CUSTOMER_ADDRESS_NUMBER_GUID AS CUSTOMER_ADDRESS_NUMBER_GUID,
CM.SOURCE_SYSTEM_ADDRESS_NUMBER AS SOURCE_SYSTEM_ADDRESS_NUMBER,
CM.COMPANY_CODE as COMPANY_CODE,
market.MARKET AS MARKET_CODE,
market.DESCRIPTION AS MARKET_DESC,
0 AS MARKET_CODE_SEQ,
submarket.SUBMARKET AS SUB_MARKET_CODE,
submarket.DESCRIPTION AS SUB_MARKET_DESC,
0	AS SUB_MARKET_CODE_SEQ,
tradeclass.TRADE_CLASS AS TRADE_CLASS_CODE,
tradeclass.DESCRIPTION AS TRADE_CLASS_DESC,
0 AS TRADE_CLASS_SEQ,
tradegroup.TRADE_GROUP AS TRADE_GROUP_CODE,
tradegroup.DESCRIPTION AS TRADE_GROUP_DESC,
0 AS TRADE_GROUP_SEQ,
tradetype.TRADE_TYPE AS TRADE_TYPE_CODE,
tradetype.DESCRIPTION AS TRADE_TYPE_DESC,
0 AS TRADE_TYPE_SEQ,
tradesector.TRADE_SECTOR AS TRADE_SECTOR_CODE,
tradesector.DESCRIPTION AS TRADE_SECTOR_DESC,
0 AS TRADE_SECTOR_SEQ,
CT.PRICEGROUP AS PRICE_GROUP,
null AS TOTAL_SO_QTY_DISCOUNT,
null AS ADDITIONAL_DISCOUNT,
CT.PDSCUSTREBATEGROUPID as CUSTOMER_REBATE_GROUP,
CT.CURRENCY AS CURRENCY,
CT.TAXGROUP AS VAT_GROUP,
null as min_order_qty_ca,
null as min_order_qty_pallets,
null as FULLPALLET,
0 as MAX_ORDER_QTY_CA,
0 as MAX_ORDER_QTY_PALLETS,
CC.DISPLAYVALUE AS FIN_DIM_COST_CENTRE,
FC.DISPLAYVALUE AS FIN_DIM_CUSTOMER,
DP.DISPLAYVALUE AS FIN_DIM_DEPARTMENT,
SI.DISPLAYVALUE AS FIN_DIM_SITE,
CURRENT_DATE as date_inserted,
CURRENT_DATE as date_updated
FROM  CM
LEFT join CT on CM.SOURCE_SYSTEM_ADDRESS_NUMBER=CT.ACCOUNTNUM and CM.COMPANY_CODE=UPPER(CT.DATAAREAID)
LEFT JOIN CC ON CT.PARTITION = CC.PARTITION AND CT.DEFAULTDIMENSION = CC.DIMENSIONATTRIBUTEVALUESET
LEFT JOIN FC ON CT.PARTITION = FC.PARTITION AND CT.DEFAULTDIMENSION = FC.DIMENSIONATTRIBUTEVALUESET
LEFT JOIN DP ON CT.PARTITION = DP.PARTITION AND CT.DEFAULTDIMENSION = DP.DIMENSIONATTRIBUTEVALUESET
LEFT JOIN SI ON CT.PARTITION = SI.PARTITION AND CT.DEFAULTDIMENSION = SI.DIMENSIONATTRIBUTEVALUESET
LEFT JOIN  MARKET ON CT.WBXMARKET=MARKET.RECID
LEFT JOIN  SUBMARKET ON CT.WBXSUBMARKET=SUBMARKET.RECID
LEFT JOIN  TRADECLASS ON TRADECLASS.RECID=CT.WBXTRADE_CLASS
LEFT JOIN  TRADEGROUP ON TRADEGROUP.RECID=CT.WBXTRADE_GROUP
LEFT JOIN  TRADETYPE ON TRADETYPE.RECID=CT.WBXTRADE_TYPE
LEFT JOIN  TRADESECTOR ON TRADESECTOR.RECID=CT.WBXTRADE_SECTOR
where upper(trim(CM.COMPANY_CODE)) in {{env_var("DBT_D365_COMPANY_FILTER")}}
),

src as(
select 
'COMPANY' as COMPANY_GENERIC_ADDRESS_TYPE,
'CUSTOMER_MAIN' as GENERIC_ADDRESS_TYPE ,
cast(substring(source_system,1,255) as text(255) ) as source_system  ,
cast(substring(source_system_address_number,1,255) as text(255) ) as source_system_address_number  ,
cast(substring(company_code,1,60) as text(60) ) as company_code  ,
cast(substring(market_code,1,60) as text(60) ) as market_code  ,
cast(substring(market_desc,1,255) as text(255) ) as market_desc  ,
cast(market_code_seq as number(38,0) ) as market_code_seq  ,
cast(substring(sub_market_code,1,60) as text(60) ) as sub_market_code  ,
cast(substring(sub_market_desc,1,255) as text(255) ) as sub_market_desc  ,
cast(sub_market_code_seq as number(38,0) ) as sub_market_code_seq  ,
cast(substring(trade_class_code,1,60) as text(60) ) as trade_class_code  ,
cast(substring(trade_class_desc,1,255) as text(255) ) as trade_class_desc  ,
cast(trade_class_seq as number(38,0) ) as trade_class_seq  ,
cast(substring(trade_group_code,1,60) as text(60) ) as trade_group_code  ,
cast(substring(trade_group_desc,1,255) as text(255) ) as trade_group_desc  ,
cast(trade_group_seq as number(38,0) ) as trade_group_seq  ,
cast(substring(trade_type_code,1,60) as text(60) ) as trade_type_code  ,
cast(substring(trade_type_desc,1,255) as text(255) ) as trade_type_desc  ,
cast(trade_type_seq as number(38,0) ) as trade_type_seq  ,
cast(substring(trade_sector_code,1,60) as text(60) ) as trade_sector_code  ,
cast(substring(trade_sector_desc,1,255) as text(255) ) as trade_sector_desc  ,
cast(trade_sector_seq as number(38,0) ) as trade_sector_seq  ,
cast(substring(price_group,1,60) as text(60) ) as price_group  ,
cast(substring(total_so_qty_discount,1,60) as text(60) ) as total_so_qty_discount  ,
cast(substring(additional_discount,1,60) as text(60) ) as additional_discount  ,
cast(substring(customer_rebate_group,1,60) as text(60) ) as customer_rebate_group  ,
cast(substring(currency,1,60) as text(60) ) as currency  ,
cast(substring(vat_group,1,60) as text(60) ) as vat_group  ,
cast(date_inserted as timestamp_ntz(9) ) as date_inserted  ,
cast(date_updated as timestamp_ntz(9) ) as date_updated  ,
cast(min_order_qty_ca as number(38,10) ) as min_order_qty_ca  ,
cast(min_order_qty_pallets as number(38,10) ) as min_order_qty_pallets  ,
cast(substring(fullpallet,1,10) as text(10) ) as full_pallet_flag  ,
cast(max_order_qty_ca as number(38,10) ) as max_order_qty_ca  ,
cast(max_order_qty_pallets as number(38,10) ) as max_order_qty_pallets  ,
cast(substring(fin_dim_cost_centre,1,255) as text(255) ) as fin_dim_cost_centre  ,
cast(substring(fin_dim_customer,1,255) as text(255) ) as fin_dim_customer  ,
cast(substring(fin_dim_department,1,255) as text(255) ) as fin_dim_department  ,
cast(substring(fin_dim_site,1,255) as text(255) ) as fin_dim_site

from set1
),

Final as(
select 
{{ dbt_utils.surrogate_key(['src.source_system','src.company_code','COMPANY_GENERIC_ADDRESS_TYPE']) }} AS COMPANY_ADDRESS_GUID,
{{ dbt_utils.surrogate_key(['src.source_system','src.source_system_address_number','GENERIC_ADDRESS_TYPE','src.COMPANY_CODE']) }} AS CUSTOMER_ADDRESS_NUMBER_GUID,

* ,ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM_ADDRESS_NUMBER, COMPANY_CODE ORDER BY 1) rowNum from src
)

select * from Final --where rownum=1