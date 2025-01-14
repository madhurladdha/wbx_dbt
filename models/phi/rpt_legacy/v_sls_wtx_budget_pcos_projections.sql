{{ config(materialized=env_var("DBT_MAT_VIEW"), tags=["sales","budget"]) }}

with cte_inventitempricesim as (select * from {{ ref('src_inventitempricesim') }}),
cte_inventitemprice as (select * from {{ ref('src_inventitemprice') }}),
cte_dim_date_oc as (select * from {{ ref('dim_wbx_date_oc') }}),
cte_dim_date as (select * from {{ ref('src_dim_date') }}),
cte_dim_planning_date_oc as (select * from {{ ref('dim_wbx_planning_date_oc') }}),
cte_inventtable as (select * from {{ ref('src_inventtable') }}),
cte_inventmodelgroupitem as (select * from {{ ref('src_inventmodelgroupitem') }}),
cte_ecoresproduct as (select * from {{ ref('src_ecoresproduct') }}),
cte_ecoresproducttranslation as (select * from {{ ref('src_ecoresproducttranslation') }}),
cte_ledger as (select * from {{ ref('src_ledger') }}),
cte_bomcalctable as (select * from {{ ref('src_bomcalctable') }}),
cte_inventdim as (select * from {{ ref('src_inventdim') }}),
cte_bomcalctrans as (select * from {{ ref('src_bomcalctrans_tbl') }}),
cte_customer_ext as (select * from {{ ref('dim_wbx_customer_ext') }}),
cte_forecast_fact as (select * from {{ ref('fct_wbx_sls_forecast') }}),
cte_item as (select * from {{ ref('dim_wbx_item') }}),
cte_item_ext as (select * from {{ ref('dim_wbx_item_ext') }}),
cte_EXC_Fact_Account_Plan_Actual as (select * from {{ ref('src_exc_fact_account_plan_actual') }}),
cte_EXC_Fact_Account_Plan as (select * from {{ ref('src_exc_fact_account_plan') }}),
cte_exc_dim_scenario as (select * from {{ ref('src_exc_dim_scenario') }}),
cte_exc_dim_pc_customer as (select * from {{ ref('src_exc_dim_pc_customer') }}),
cte_exc_dim_pc_product as (select * from {{ ref('src_exc_dim_pc_product') }}),
--Find latest Pending Versions for each company
 MAX_VERSION_ID AS (SELECT DATAAREAID,LEFT(VERSIONID,5) AS LOCATION,MAX(VERSIONID) AS VERSIONID FROM cte_INVENTITEMPRICESIM T1 WHERE RIGHT(VERSIONID,4) = (
   SELECT MAX(RIGHT(VERSIONID,4)) FROM cte_INVENTITEMPRICE WHERE UPPER(VERSIONID) LIKE 'STDBL%') GROUP BY DATAAREAID,LEFT(VERSIONID,5)),
 --Find start date for ecah financial year using latest pending Version
   VERDATE AS (SELECT DISTINCT V.VERSIONID,D.FISCAL_YEAR_BEGIN_DT FROM cte_dim_date_oc D INNER JOIN MAX_VERSION_ID V ON D.FISCAL_YEAR = RIGHT(V.VERSIONID,4)),
MAX_ICV AS (
    SELECT MAX(T1.MODIFIEDDATETIME) AS MAXMODIFIEDDATETIME,
    --MAX(T1.ACTIVATIONDATE) AS MAXACTIVATIONDATE,
    T1.ITEMID AS ITEMID,
    T1.VERSIONID AS VERSIONID,
    T1.PRICETYPE AS PRICETYPE,
    T1.INVENTDIMID AS INVENTDIMID,
    T1.DATAAREAID AS DATAAREAID,
    T1.PARTITION AS PARTITION,
    1010 AS RECID 
    FROM cte_INVENTITEMPRICE T1 INNER JOIN
     MAX_VERSION_ID T2 ON T1.DATAAREAID = T2.DATAAREAID AND T1.VERSIONID = T2.VERSIONID
    WHERE T1.PRICETYPE = 0
    GROUP BY T1.ITEMID,T1.VERSIONID,T1.PRICETYPE,T1.INVENTDIMID,T1.DATAAREAID,T1.PARTITION),
 ICV AS (
    SELECT IIP.VERSIONID,
    IIP.ITEMID,
    erptProduct.NAME AS DESCRIPTION,
    IIP.PRICECALCID,
    IIP.PRICE,
    IIP.DATAAREAID,
    IIP.PARTITION,
    --IIP.ACTIVATIONDATE,
    --IIP.CREATEDDATETIME,
    CASE WHEN UPPER(IMGI.MODELGROUPID) IN('FG-STD','DRINKS') THEN

   NVL (DATEADD(second,-1,LEAD (IIP.MODIFIEDDATETIME) OVER (PARTITION BY IIP.ITEMID, NVL(ID.INVENTSITEID,'') ORDER BY IIP.ITEMID,

    NVL(ID.INVENTSITEID,''),IIP.MODIFIEDDATETIME)),CAST('2050-12-31' AS DATETIME))

  ELSE

  NVL (DATEADD(second,-1,LEAD (IIP.MODIFIEDDATETIME) OVER (PARTITION BY IIP.ITEMID,NVL(ID.INVENTSIZEID,''), NVL(ID.INVENTSITEID,'') ORDER BY IIP.ITEMID,NVL(ID.INVENTSIZEID,''),

   NVL(ID.INVENTSITEID,''),IIP.MODIFIEDDATETIME)),CAST('2050-12-31' AS DATETIME))

   END as EXPIR_DATE,
    IIP.MODIFIEDDATETIME,
    --CASE WHEN IIP.CREATEDDATETIME = MI.MAXCREATEDDATETIME AND IIP.ACTIVATIONDATE = MI.MAXACTIVATIONDATE THEN 'YES' ELSE 'NO' END AS ACTIVE,
    'YES' AS ACTIVE,
    NVL(ID.INVENTSIZEID,'') AS VARIANT,
    NVL(ID.INVENTSITEID,'') AS STOCK_SITE,
    L.ACCOUNTINGCURRENCY,
    CAST(IIP.PRICE/CAST(IIP.PRICEUNIT AS FLOAT) AS FLOAT) AS UNIT_PRICE
    FROM cte_INVENTITEMPRICE IIP 
    INNER JOIN cte_inventtable IT ON IIP.DATAAREAID = IT.DATAAREAID AND IIP.ITEMID = IT.ITEMID AND IIP.PARTITION = IT.PARTITION
    INNER JOIN cte_INVENTMODELGROUPITEM IMGI ON IT.ITEMID = IMGI.ITEMID AND IT.DATAAREAID = IMGI.ITEMDATAAREAID AND IT.PARTITION = IMGI.PARTITION
    INNER JOIN cte_ECORESPRODUCT erpProduct ON IT.PRODUCT = erpProduct.RECID AND IT.PARTITION = erpProduct.PARTITION
    INNER JOIN cte_ecoresproducttranslation erptProduct ON erptProduct.product = erpProduct.recid AND erptProduct.PARTITION = erptProduct.PARTITION
    INNER JOIN MAX_ICV MI ON IIP.ITEMID = MI.ITEMID AND IIP.INVENTDIMID = MI.INVENTDIMID AND IIP.VERSIONID = MI.VERSIONID AND IIP.DATAAREAID = MI.DATAAREAID AND IIP.PARTITION = MI.PARTITION
     AND IIP.MODIFIEDDATETIME = MI.MAXMODIFIEDDATETIME
    INNER JOIN cte_LEDGER L ON UPPER(IIP.DATAAREAID) = L.NAME
    LEFT OUTER JOIN cte_BOMCALCTABLE BT ON IIP.PRICECALCID = BT.PRICECALCID AND IIP.DATAAREAID = BT.DATAAREAID AND IIP.PARTITION = BT.PARTITION
    LEFT OUTER JOIN cte_INVENTDIM ID ON IIP.INVENTDIMID = ID.INVENTDIMID AND IIP.PARTITION = ID.PARTITION AND IIP.DATAAREAID = ID.DATAAREAID
    --WHERE IIP.ACTIVATIONDATE <= TO_DATE(CONVERT_TIMEZONE('UTC',current_timestamp)) AND IIP.PRICETYPE = 0),
    WHERE IIP.PRICETYPE = 0),
 expBCT 
    (SOURCE_BOM_PATH,
     VERSION_ID,
     ROOT_SRC_ITEM_IDENTIFIER,
     DESCRIPTION,
     PRICECALCID,
     ACTIVE_FLAG,
     EFF_DATE,
     CREATION_DATE_TIME,
     EXPIR_DATE,
     SOURCE_UPDATED_DATETIME,
     TRANSACTION_CURRENCY,
     COMP_BOM_LEVEL,
     COMP_SRC_ITEM_IDENTIFIER,
	 COMP_COST_GROUP_ID,
     CALCTYPE,
     COMP_CALCTYPE_DESC,
     CONSISTOFPRICE,
     CONSUMPTIONVARIABLE,
     COSTPRICEQTY,
     COSTPRICE,
     COSTPRICEUNIT,
     ROOT_COMPANY_CODE,
     PARTITION,
	 CONSUMPTION,
     QTY,
     TRANSACTION_UOM,
     EXP_COSTPRICEQTY,
     EXP_COSTPRICE,
     PARENT_ITEM_INDICATOR,
     INVENT_DIM,SOURCE_BUSINESS_UNIT_CODE,
     COMP_SRC_VARIANT_CODE,ROOT_SRC_VARIANT_CODE,
     STOCK_SITE,
     ROOT_SRC_UNIT_PRICE) 
    AS 
    (SELECT CAST(CONCAT(BCT.PRICECALCID, '-', CAST(BCT.LINENUM AS VARCHAR(255))) AS VARCHAR(255)),
    ICV.VERSIONID,
    ICV.ITEMID,
    ICV.DESCRIPTION,
    ICV.PRICECALCID,
    ICV.ACTIVE,
    D.FISCAL_YEAR_BEGIN_DT,
    ICV.MODIFIEDDATETIME,
    ICV.EXPIR_DATE,
    ICV.MODIFIEDDATETIME,
    ICV.ACCOUNTINGCURRENCY,
    NVL(BCT.LEVEL_,0) AS LEVEL,
    NVL(BCT.RESOURCE_,'') AS MATERIAL,
	NVL(BCT.COSTGROUPID,'') AS COSTGROUPID,
    NVL(BCT.CALCTYPE,1) AS CALCTYPE, 
    CASE NVL(BCT.CALCTYPE,0) 
      WHEN 0 THEN 'Production' WHEN 1 THEN 'Item' WHEN 2 THEN 'BOM' WHEN 3 THEN 'Service' WHEN 4 THEN 'Setup' WHEN 5 THEN 'Process' WHEN 6 THEN 'Quantity'
	  WHEN 7 THEN 'Surcharge' WHEN 8 THEN 'Rate' WHEN 9 THEN 'Cost Group' WHEN 10 THEN 'Output unit based' WHEN 14 THEN 'Input unit based' WHEN 15 THEN 'Purchase'
	  WHEN 16 THEN 'Unit based purchase overheads' WHEN 20 THEN 'Burden' WHEN 21 THEN 'Batch'
	  ELSE '' END AS CALCTYPEDESC,
    NVL(BCT.CONSISTOFPRICE,'') AS CONSISTOFPRICE,
    --NVL(BCT.COSTPRICE,ICV.PRICE) AS PRICE,
	NVL(BCT.CONSUMPTIONVARIABLE,0) AS CONSUMPTIONVARIABLE,
    NVL(BCT.COSTPRICEQTY,ICV.PRICE) AS COSTPRICEQTY,
    NVL(BCT.COSTPRICE,ICV.PRICE) AS COSTPRICE,
    NVL(BCT.COSTPRICEUNIT,1) AS COSTPRICEUNIT,
    ICV.DATAAREAID,
    ICV.PARTITION,
    CAST(NVL(BCT.CONSUMPTIONVARIABLE, 0) AS FLOAT) AS CONSUMPTION,
    CAST(NVL(BCT.QTY, 0) AS NUMBER(38, 10)) AS QTY, 
    NVL(BCT.UNITID,'Case') AS UNITID,
	CAST(NVL(BCT.COSTPRICEQTY, ICV.PRICE) AS FLOAT) AS EXP_COSTPRICEQTY,
    CAST(NVL(BCT.COSTPRICE,ICV.PRICE) AS FLOAT) AS EXP_COSTPRICE,
	CASE WHEN NVL(BCT.CALCTYPE, 1) IN(0, 9) OR NVL(BCT.CONSISTOFPRICE,'') <> '' THEN 'Parent' ELSE 'Item' END AS ParentOrItem,
    NVL(BCT.INVENTDIMSTR,'') AS INVENT_DIM,
    NVL(ID.INVENTLOCATIONID,'') AS PLANT,
    NVL(ID.INVENTSIZEID,'') AS VARIANT,
    '', --ICV.VARIANT,
    ICV.STOCK_SITE,
    ICV.UNIT_PRICE
    FROM ICV
    INNER JOIN VERDATE D ON ICV.VERSIONID = D.VERSIONID
    LEFT OUTER JOIN cte_BOMCALCTRANS BCT ON ICV.PRICECALCID = BCT.PRICECALCID AND ICV.DATAAREAID = BCT.DATAAREAID AND ICV.PARTITION = BCT.PARTITION
 	LEFT OUTER JOIN cte_INVENTDIM ID ON BCT.INVENTDIMID = ID.INVENTDIMID AND BCT.PARTITION = ID.PARTITION AND BCT.DATAAREAID = ID.DATAAREAID
	
    UNION ALL
    
    SELECT CAST(CONCAT(EB.SOURCE_BOM_PATH, '/', BCT.PRICECALCID, '-', CAST(BCT.LINENUM AS VARCHAR(4000))) AS VARCHAR(255)),
    EB.VERSION_ID,
    EB.ROOT_SRC_ITEM_IDENTIFIER,
    EB.DESCRIPTION,
    BCT.PRICECALCID,
    EB.ACTIVE_FLAG,
    EB.EFF_DATE,
    EB.CREATION_DATE_TIME,
    EB.EXPIR_DATE,
    EB.SOURCE_UPDATED_DATETIME,
    EB.TRANSACTION_CURRENCY,
    BCT.LEVEL_ + EB.COMP_BOM_LEVEL AS COMP_BOM_LEVEL,
    BCT.RESOURCE_,
	BCT.COSTGROUPID,
    BCT.CALCTYPE AS CALCTYPE, 
    CASE BCT.CALCTYPE 
	  WHEN 0 THEN 'Production' WHEN 1 THEN 'Item' WHEN 2 THEN 'BOM' WHEN 3 THEN 'Service' WHEN 4 THEN 'Setup' WHEN 5 THEN 'Process' WHEN 6 THEN 'Quantity'
	  WHEN 7 THEN 'Surcharge' WHEN 8 THEN 'Rate' WHEN 9 THEN 'Cost Group' WHEN 10 THEN 'Output unit based' WHEN 14 THEN 'Input unit based' WHEN 15 THEN 'Purchase'
	  WHEN 16 THEN 'Unit based purchase overheads' WHEN 20 THEN 'Burden' WHEN 21 THEN 'Batch'
	  ELSE '' END AS CALCTYPEDESC,BCT.CONSISTOFPRICE AS CONSISTOFPRICE,
      --NVL(BCT.COSTPRICE,ICV.PRICE) AS PRICE,
	  BCT.CONSUMPTIONVARIABLE,
      BCT.COSTPRICEQTY,
      BCT.COSTPRICE,
      BCT.COSTPRICEUNIT,
      BCT.DATAAREAID,
      BCT.PARTITION,
      CAST((BCT.CONSUMPTIONVARIABLE * EB.CONSUMPTION / NULLIF(EB.QTY, 0)) AS FLOAT) AS CONSUMPTION,
      CAST(BCT.QTY AS NUMBER(38, 10)) AS QTY,
      BCT.UNITID,
      CAST((BCT.COSTPRICEQTY * EB.CONSUMPTION / NULLIF(EB.QTY, 0)) AS FLOAT) AS EXP_COSTPRICEQTY,
      CAST((BCT.COSTPRICE * EB.CONSUMPTION / NULLIF(EB.QTY, 0)) AS FLOAT) AS EXP_COSTPRICE,
	  CASE WHEN BCT.CALCTYPE IN(0, 9) OR BCT.CONSISTOFPRICE <> '' THEN 'Parent' ELSE 'Item' END AS ParentOrItem,BCT.INVENTDIMSTR AS INVENT_DIM,
      EB.SOURCE_BUSINESS_UNIT_CODE,
      EB.COMP_SRC_VARIANT_CODE,
      EB.ROOT_SRC_VARIANT_CODE,
      EB.STOCK_SITE,
      EB.ROOT_SRC_UNIT_PRICE
     FROM cte_BOMCALCTRANS BCT 
	  INNER JOIN expBCT EB ON EB.ROOT_COMPANY_CODE = BCT.DATAAREAID AND BCT.PARTITION = BCT.PARTITION AND BCT.PRICECALCID = EB.CONSISTOFPRICE),
 MFG_WTX_CBOMP_FACT AS (  
   SELECT '{{env_var('DBT_SOURCE_SYSTEM')}}' AS SOURCE_SYSTEM,
   ACTIVE_FLAG,
   STOCK_SITE,
   VERSION_ID,
   SOURCE_BOM_PATH,
   EFF_DATE,
   CREATION_DATE_TIME,
   EXPIR_DATE,
   SOURCE_UPDATED_DATETIME,
   TRANSACTION_CURRENCY,
   TRANSACTION_UOM,
   ROOT_COMPANY_CODE,
   ROOT_SRC_ITEM_IDENTIFIER,
   ROOT_SRC_VARIANT_CODE,
   ROOT_SRC_UNIT_PRICE,
   CASE WHEN COMP_BOM_LEVEL = 0 AND COMP_SRC_ITEM_IDENTIFIER = '' THEN ROOT_SRC_ITEM_IDENTIFIER ELSE COMP_SRC_ITEM_IDENTIFIER END AS COMP_SRC_ITEM_IDENTIFIER,

       CASE WHEN COMP_BOM_LEVEL = 0 AND COMP_SRC_ITEM_IDENTIFIER = '' THEN ROOT_SRC_VARIANT_CODE ELSE COMP_SRC_VARIANT_CODE END AS COMP_SRC_VARIANT_CODE,

       CONSUMPTION AS COMP_CONSUMPTION_QTY,QTY AS COMP_CONSUMPTION_UNIT,COSTPRICE AS COMP_COST_PRICE,COSTPRICEUNIT AS COMP_COST_PRICE_UNIT,

       CASE ROUND(QTY,4) WHEN 0 THEN CASE COMP_BOM_LEVEL WHEN 0 THEN COSTPRICE / COSTPRICEUNIT ELSE 0 END ELSE (CAST(CONSUMPTION AS FLOAT) * COSTPRICE) / (CAST(QTY * COSTPRICEUNIT AS FLOAT)) END AS COMP_ITEM_UNIT_COST,
   COMP_BOM_LEVEL,
   COMP_CALCTYPE_DESC,
   COMP_COST_GROUP_ID,
   PARENT_ITEM_INDICATOR,SOURCE_BUSINESS_UNIT_CODE
 FROM expBCT),
--  ***** MFG_WTX_CBOMP_FACT Finish *****

--Sample data table
SALES_DATA_SET AS (SELECT DISTINCT CALENDAR_DATE AS CALENDAR_DATE,SOURCE_ITEM_IDENTIFIER,'NSP' AS VARIANT_CODE,PLAN_SOURCE_CUSTOMER_CODE AS PLAN_SOURCE_CUSTOMER_CODE
FROM cte_forecast_fact FCST 
 WHERE SNAPSHOT_DATE IN ('20-MAY-2023')
 ),

--***** Based on V_WTX_MFG_CBOM_VARIANT query *****
--with ** Required if MFG_WTX_CBOMP_FACT above is now a table
CBOM AS (SELECT 
 SOURCE_SYSTEM,ROOT_COMPANY_CODE,STOCK_SITE,VERSION_ID,EFF_DATE,CREATION_DATE_TIME,EXPIR_DATE,ROOT_SRC_ITEM_IDENTIFIER,ROOT_SRC_VARIANT_CODE,ROOT_SRC_UNIT_PRICE,GL_UNIT_PRICE
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(NVL ("'CG'",0),NumDecs) ELSE NVL ("'CG'",0) END AS RAW_MATERIALS 
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(NVL ("'CG_PACK'",0),NumDecs) ELSE NVL ("'CG_PACK'",0) END AS PACKAGING
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(NVL ("'LAB'",0),NumDecs) ELSE NVL ("'LAB'",0) END AS LABOUR
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(NVL ("'BI'",0),NumDecs) ELSE NVL ("'BI'",0) END AS BOUGHT_IN
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(NVL ("'CO'",0),NumDecs) ELSE NVL ("'CO'",0) END AS CO_PACK
 ,CASE STOCK_SITE WHEN 'WBX-CBY' THEN ROUND(GL_UNIT_PRICE - (ROUND(ROUND(NVL ("'CG'",0),NumDecs) + ROUND(NVL ("'CG_PACK'",0),NumDecs) + ROUND(NVL ("'LAB'",0),NumDecs) + ROUND(NVL ("'BI'",0),NumDecs) + 
    ROUND(NVL ("'CO'",0),NumDecs),NumDecs)),NumDecs) ELSE 0 END AS OTHER,
 CASE MIN(ROOT_SRC_VARIANT_CODE) OVER(PARTITION BY SOURCE_SYSTEM,ROOT_COMPANY_CODE,ROOT_SRC_ITEM_IDENTIFIER ORDER BY EFF_DATE DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) WHEN '' THEN 0 ELSE 1 END AS VARIANT_FLAG         
 FROM 
(SELECT SOURCE_SYSTEM,ROOT_COMPANY_CODE,STOCK_SITE,VERSION_ID,NVL (X.BL_EFF_DATE,X.EFF_DATE) AS EFF_DATE,CREATION_DATE_TIME,EXPIR_DATE,ROOT_SRC_ITEM_IDENTIFIER,ROOT_SRC_VARIANT_CODE,X.NumDecs,
UPPER(CASE WHEN X.COMP_COST_GROUP_ID = '' AND X.COMP_BOM_LEVEL = 0 THEN 'BI'
       WHEN X.COMP_COST_GROUP_ID = '' AND X.COMP_BOM_LEVEL <> 0 THEN 'CO' 
  WHEN UPPER(X.COMP_COST_GROUP_ID) = 'MO' THEN 'CG' WHEN UPPER(X.COMP_COST_GROUP_ID)  = 'TRAN' THEN 'CG' ELSE X.COMP_COST_GROUP_ID END) AS COMP_COST_GROUP_ID,
MAX(ROOT_SRC_UNIT_PRICE) AS ROOT_SRC_UNIT_PRICE,MAX(ROUND(ROOT_SRC_UNIT_PRICE,NumDecs)) AS GL_UNIT_PRICE,
  SUM(CASE WHEN X.COMP_COST_GROUP_ID = '' AND X.COMP_BOM_LEVEL = 0 THEN ROOT_SRC_UNIT_PRICE ELSE COMP_ITEM_UNIT_COST END) AS COMP_ITEM_UNIT_COST
   FROM
--Derived CBY table to find Corby BOM's and to also find the BL effective date for the Corby SKU
(SELECT BOM.*,ITM.MANGRPCD_COPACK_FLAG,CBY.BL_EFF_DATE,NVL (CBY.NumDecs,2) AS NumDecs FROM MFG_WTX_CBOMP_FACT BOM --POSTSNOWP.R_EI_SYSADM.MFG_WTX_CBOM_FACT
LEFT OUTER JOIN (SELECT DISTINCT RIGHT(F1.VERSION_ID,4) AS VERISON_YEAR,F1.ROOT_SRC_ITEM_IDENTIFIER,ROUND(F1.ROOT_SRC_UNIT_PRICE,2) AS ROOT_SRC_UNIT_PRICE,F1.EFF_DATE,
FIRST_VALUE(F2.EFF_DATE) OVER (PARTITION BY RIGHT(F1.VERSION_ID,4),F1.ROOT_SRC_ITEM_IDENTIFIER,ROUND(F1.ROOT_SRC_UNIT_PRICE,2),F1.EFF_DATE ORDER BY ABS(DATEDIFF(second,F1.EFF_DATE,F2.EFF_DATE))) AS BL_EFF_DATE
 ,CASE WHEN len(REVERSE(CAST(FLOOR(REVERSE(ABS(F2.ROOT_SRC_UNIT_PRICE))) AS bigint))) < 2 THEN 2 ELSE len(REVERSE(CAST(FLOOR(REVERSE(ABS(F2.ROOT_SRC_UNIT_PRICE))) AS bigint))) END as NumDecs
                 FROM MFG_WTX_CBOMP_FACT F1 --POSTSNOWP.R_EI_SYSADM.MFG_WTX_CBOM_FACT
  INNER JOIN MFG_WTX_CBOMP_FACT F2 ON RIGHT(F1.VERSION_ID,4) = RIGHT(F2.VERSION_ID,4) AND F1.ROOT_SRC_ITEM_IDENTIFIER = F2.ROOT_SRC_ITEM_IDENTIFIER --POSTSNOWP.R_EI_SYSADM.MFG_WTX_CBOM_FACT
   AND ROUND(F1.ROOT_SRC_UNIT_PRICE,2) = ROUND(F2.ROOT_SRC_UNIT_PRICE,2) AND UPPER(F2.STOCK_SITE) = 'WBX-BL'
  WHERE UPPER(F1.STOCK_SITE) IN('WBX-CBY') AND F1.COMP_BOM_LEVEL <> 0) AS CBY
   ON RIGHT(BOM.VERSION_ID,4) = CBY.VERISON_YEAR AND BOM.ROOT_SRC_ITEM_IDENTIFIER = CBY.ROOT_SRC_ITEM_IDENTIFIER AND ROUND(BOM.ROOT_SRC_UNIT_PRICE,2) = CBY.ROOT_SRC_UNIT_PRICE AND (BOM.EFF_DATE = CBY.EFF_DATE
    OR BOM.EFF_DATE = CBY.BL_EFF_DATE)
   -- Derived table ITM, likely to change once updates to item tables have been completed
  INNER JOIN (SELECT IM.SOURCE_ITEM_IDENTIFIER,MAX(CASE WHEN NVL (UPPER(E.MANGRPCD_SITE),'BL') IN('BL','BL/CBY','CBY','WEETABIX/ORG') THEN 'N' ELSE 'Y' END) AS MANGRPCD_COPACK_FLAG 
  FROM cte_item IM
   LEFT OUTER JOIN cte_item_ext E ON IM.ITEM_GUID = E.ITEM_GUID 
     WHERE IM.SOURCE_SYSTEM = '{{env_var('DBT_SOURCE_SYSTEM')}}' AND IS_REAL(try_to_numeric(IM.SOURCE_ITEM_IDENTIFIER)) = 1 AND LEN(IM.SOURCE_ITEM_IDENTIFIER) = 5 GROUP BY IM.SOURCE_ITEM_IDENTIFIER) ITM
     ON BOM.ROOT_SRC_ITEM_IDENTIFIER = ITM.SOURCE_ITEM_IDENTIFIER
  WHERE ((UPPER(BOM.STOCK_SITE) NOT IN('WBX-CBY') AND CBY.ROOT_SRC_ITEM_IDENTIFIER IS NULL) OR (UPPER(BOM.STOCK_SITE) = 'WBX-CBY' AND COMP_BOM_LEVEL <> 0 AND CBY.ROOT_SRC_ITEM_IDENTIFIER IS NOT NULL))
      AND ((COMP_BOM_LEVEL = 1 AND
                                      (UPPER(COMP_CALCTYPE_DESC) IN('ITEM','SERVICE') OR (UPPER(COMP_CALCTYPE_DESC) IN('BOM') AND UPPER(PARENT_ITEM_INDICATOR) in('ITEM','PARENT'))))
                   OR (COMP_BOM_LEVEL = 0 AND UPPER(COMP_CALCTYPE_DESC) IN('PRODUCTION') AND UPPER(PARENT_ITEM_INDICATOR) = 'ITEM')
                   )
  ) X
  GROUP BY SOURCE_SYSTEM,ROOT_COMPANY_CODE,STOCK_SITE,VERSION_ID,NVL (X.BL_EFF_DATE,X.EFF_DATE),CREATION_DATE_TIME,EXPIR_DATE,ROOT_SRC_ITEM_IDENTIFIER,ROOT_SRC_VARIANT_CODE,X.NumDecs,
UPPER(CASE WHEN X.COMP_COST_GROUP_ID = '' AND X.COMP_BOM_LEVEL = 0 THEN 'BI'
       WHEN X.COMP_COST_GROUP_ID = '' AND X.COMP_BOM_LEVEL <> 0 THEN 'CO' 
  WHEN UPPER(X.COMP_COST_GROUP_ID) = 'MO' THEN 'CG' WHEN UPPER(X.COMP_COST_GROUP_ID)  = 'TRAN' THEN 'CG' ELSE X.COMP_COST_GROUP_ID END)) AS SourceTable
PIVOT  
(  
sum(comp_item_unit_cost)  
FOR COMP_COST_GROUP_ID IN ('CG', 'CG_PACK','LAB', 'BI', 'CO','OTH')  
) AS PivotTable),
cte_v_sls_wtx_budget_pcos_assign as --pending due to item_guid issue in sales forecast fact model
(
    SELECT DISTINCT
SDS.PLAN_SOURCE_CUSTOMER_CODE,
SDS.SOURCE_ITEM_IDENTIFIER,
SDS.VARIANT_CODE,
SDS.CALENDAR_DATE,
'CA' AS TRANS_UOM,'GBP' AS BASE_CURRENCY
,FIRST_VALUE(CBOM.RAW_MATERIALS) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_ING_COST
,FIRST_VALUE(CBOM.PACKAGING) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_PKG_COST
,FIRST_VALUE(CBOM.LABOUR) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_LBR_COST
,FIRST_VALUE(CBOM.BOUGHT_IN) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_BOUGHT_IN_COST
,FIRST_VALUE(CBOM.OTHER) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_OTH_COST
,FIRST_VALUE(CBOM.CO_PACK) OVER (PARTITION BY UPPER(CBOM.ROOT_COMPANY_CODE),UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) ORDER BY CBOM.EFF_DATE DESC) AS BASE_EXT_COPACK_COST
FROM SALES_DATA_SET SDS
LEFT OUTER JOIN CBOM ON UPPER(CBOM.ROOT_COMPANY_CODE) = UPPER('WBX') AND UPPER(SDS.SOURCE_ITEM_IDENTIFIER) = UPPER(CBOM.ROOT_SRC_ITEM_IDENTIFIER) 
  AND (CBOM.VARIANT_FLAG = 0 OR (CBOM.VARIANT_FLAG = 1 AND UPPER(SDS.VARIANT_CODE) = UPPER(CBOM.ROOT_SRC_VARIANT_CODE)))
WHERE CBOM.EFF_DATE <= SDS.CALENDAR_DATE
--Sort only required for test purposes
ORDER BY 1,2,3,4
),
cte_v_sls_wtx_onpromo as --logic of the view POSTSNOWP.EI_RDM.V_SLS_WTX_ONPROMO
(
    SELECT distinct TRIM(CUST.CODE) AS CUSTOMER, TRIM(PROD.CODE) AS SOURCE_ITEM_IDENTIFIER, date_trunc('WEEK', TO_DATE(TO_CHAR(FACT.DAY_IDX),'YYYYMMDD')+1)-1 DATE
FROM 
(SELECT * FROM cte_EXC_Fact_Account_Plan_Actual WHERE 
  date_trunc('WEEK', TO_DATE(TO_CHAR(DAY_IDX),'YYYYMMDD')+1)-1 <=DATE_TRUNC('WEEK', CURRENT_DATE)-1
 UNION
 SELECT * FROM cte_EXC_Fact_Account_Plan WHERE
 date_trunc('WEEK', TO_DATE(TO_CHAR(DAY_IDX),'YYYYMMDD')+1)-1 <=DATE_TRUNC('WEEK', CURRENT_DATE)-1) FACT
LEFT OUTER JOIN cte_exc_dim_scenario SCEN
ON FACT.SCEN_IDX = SCEN.SCEN_IDX
LEFT OUTER JOIN cte_exc_dim_pc_customer CUST
ON FACT.CUST_IDX = CUST.IDX
LEFT OUTER JOIN cte_exc_dim_pc_product  PROD
ON FACT.SKU_IDX = PROD.IDX
WHERE SCEN.SCEN_IDX=1
AND FACT.ISONPROMO_SI=TRUE
),
cte_v_wtx_cust_planning as --cte for the view POSTSNOWP.EI_RDM.v_wtx_cust_planning
(
    select * from {{ref('dim_wbx_cust_planning')}}
),
cte_final as 
(
     SELECT FCST.SOURCE_SYSTEM,
  FCST.SOURCE_ITEM_IDENTIFIER,
  FCST.PLAN_SOURCE_CUSTOMER_CODE,
  FCST.CALENDAR_DATE,
  FCST.SNAPSHOT_DATE,
  FCST.ISONPROMO_SI,
  FCST.ISONPROMO_SO,
  FCST.FCF_TOT_VOL_KG,
  FCST.FCF_TOT_VOL_CA,
  FCST.FCF_TOT_VOL_UL,
  FCST.FCF_BASE_VOL_KG,
  FCST.FCF_BASE_VOL_CA ,
  FCST.FCF_BASE_VOL_UL,
  FCST.FCF_PROMO_VOL_KG,
  FCST.FCF_PROMO_VOL_CA,
  FCST.FCF_PROMO_VOL_UL,
  (CASE WHEN PRM.CUSTOMER IS NULL THEN '0' ELSE '1' END) AS ISONPROMO_FCST,
---PCOS Costs and calculated Amounts 
PCOS.BASE_EXT_ING_COST,
PCOS.BASE_EXT_PKG_COST,
PCOS.BASE_EXT_LBR_COST,
PCOS.BASE_EXT_BOUGHT_IN_COST,
PCOS.BASE_EXT_OTH_COST,
PCOS.BASE_EXT_COPACK_COST,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_ING_COST AS BASE_EXT_ING_AMT,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_PKG_COST AS BASE_EXT_PKG_AMT,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_LBR_COST AS BASE_EXT_LBR_AMT,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_BOUGHT_IN_COST AS BASE_EXT_BOUGHT_IN_AMT,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_OTH_COST AS BASE_EXT_OTH_AMT,
FCST.FCF_TOT_VOL_CA*PCOS.BASE_EXT_COPACK_COST AS BASE_EXT_COPACK_AMT,
---Dimensional attributes---------------  
DT.REPORT_FISCAL_YEAR,
DT.REPORT_FISCAL_YEAR_PERIOD_NO,
DT.FISCAL_YEAR_BEGIN_DT,
DT.FISCAL_YEAR_END_DT,
DTP.PLANNING_WEEK_CODE,
DTP.PLANNING_WEEK_START_DT,
DTP.PLANNING_WEEK_END_DT,
DTP.PLANNING_WEEK_NO,
DTP.PLANNING_MONTH_CODE,
DTP.PLANNING_MONTH_START_DT,
DTP.PLANNING_MONTH_END_DT,
DTP.PLANNING_QUARTER_NO,
DTP.PLANNING_QUARTER_START_DT,
DTP.PLANNING_QUARTER_END_DT,
DTP.PLANNING_YEAR_NO,
DTP.PLANNING_YEAR_START_DT,
DTP.PLANNING_YEAR_END_DT,
PLAN.MARKET AS MARKET,
PLAN.SUB_MARKET AS SUBMARKET,
PLAN.TRADE_CLASS AS TRADE_CLASS,
PLAN.TRADE_GROUP AS TRADE_GROUP,
PLAN.TRADE_TYPE AS TRADE_TYPE,
PLAN.TRADE_SECTOR_DESC AS TRADE_SECTOR,
ITM_EXT.DESCRIPTION,
ITM_EXT.ITEM_TYPE, 
ITM_EXT.BRANDING_DESC, 
ITM_EXT.PRODUCT_CLASS_DESC, 
ITM_EXT.SUB_PRODUCT_DESC, 
ITM_EXT.STRATEGIC_DESC, 
ITM_EXT.POWER_BRAND_DESC, 
ITM_EXT.MANUFACTURING_GROUP_DESC, 
ITM_EXT.CATEGORY_DESC, 
ITM_EXT.PACK_SIZE_DESC, 
ITM_EXT.SUB_CATEGORY_DESC,
ITM_EXT.PROMO_TYPE_DESC,
NVL(ITM_EXT.DUMMY_PRODUCT_FLAG,0) AS DUMMY_PRODUCT_FLAG
FROM cte_forecast_fact FCST 
LEFT JOIN cte_v_sls_wtx_budget_pcos_assign PCOS
	ON FCST.PLAN_SOURCE_CUSTOMER_CODE = PCOS.PLAN_SOURCE_CUSTOMER_CODE
	AND FCST.SOURCE_ITEM_IDENTIFIER = PCOS.SOURCE_ITEM_IDENTIFIER
	AND FCST.CALENDAR_DATE = PCOS.CALENDAR_DATE
	AND 'NSP' = PCOS.VARIANT_CODE
LEFT JOIN cte_dim_date DT
    ON FCST.CALENDAR_DATE = DT.CALENDAR_DATE
LEFT JOIN cte_dim_planning_date_oc DTP
    ON FCST.SOURCE_SYSTEM = DTP.SOURCE_SYSTEM
    AND FCST.CALENDAR_DATE = DTP.CALENDAR_DATE
LEFT JOIN (SELECT SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER, MAX(DUMMY_PRODUCT_FLAG) AS DUMMY_PRODUCT_FLAG, MAX(ITEM_TYPE) AS ITEM_TYPE, MAX(BRANDING_DESC) AS BRANDING_DESC, MAX(PRODUCT_CLASS_DESC) AS PRODUCT_CLASS_DESC, MAX(SUB_PRODUCT_DESC) AS SUB_PRODUCT_DESC
            , MAX(STRATEGIC_DESC) AS STRATEGIC_DESC, MAX(POWER_BRAND_DESC) AS POWER_BRAND_DESC, MAX(MANUFACTURING_GROUP_DESC) AS MANUFACTURING_GROUP_DESC
            , MAX(CATEGORY_DESC) AS CATEGORY_DESC, MAX(PACK_SIZE_DESC) AS PACK_SIZE_DESC, MAX(SUB_CATEGORY_DESC) AS SUB_CATEGORY_DESC
            , MAX(CONSUMER_UNITS_IN_TRADE_UNITS) AS CONSUMER_UNITS_IN_TRADE_UNITS, MAX(PROMO_TYPE_DESC) AS PROMO_TYPE_DESC, MAX(CONSUMER_UNITS) AS CONSUMER_UNITS
			, MAX(DESCRIPTION) AS DESCRIPTION
            FROM cte_item_ext 
            GROUP BY SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER) ITM_EXT
    ON FCST.SOURCE_SYSTEM = ITM_EXT.SOURCE_SYSTEM
    AND FCST.SOURCE_ITEM_IDENTIFIER = ITM_EXT.SOURCE_ITEM_IDENTIFIER
LEFT JOIN cte_V_WTX_CUST_PLANNING PLAN
    ON TRIM(FCST.PLAN_SOURCE_CUSTOMER_CODE) = TRIM(PLAN.TRADE_TYPE_CODE)
----View that references Exceedra tables directly and identifies Cust/SKU/Week that are on promotion for the week or not.
----For setting the flags above for Promo / Non-Promo.  Used for both Actuals and Forecast.
LEFT JOIN cte_V_SLS_WTX_ONPROMO PRM
  on PRM.customer = TRIM(FCST.PLAN_SOURCE_CUSTOMER_CODE)
  and PRM.source_item_identifier = TRIM(FCST.SOURCE_ITEM_IDENTIFIER)
  and PRM.date = DATEADD('DAY', -1, DATE_TRUNC('WEEK', DATEADD('DAY', 1, FCST.CALENDAR_DATE))) 
  //want to join on every day in the week that a promotion is running.  Adjusted the week to Sunday to align weeks.
    ---adding date filter to limit the actuals two the prior 2 years plus YTD---
WHERE FCST.SNAPSHOT_DATE IN ('22-MAY-2021','24-MAY-2021','21-MAY-2022','20-MAY-2023')
)
select 
    SOURCE_SYSTEM,
	SOURCE_ITEM_IDENTIFIER,
	PLAN_SOURCE_CUSTOMER_CODE,
	CALENDAR_DATE,
	SNAPSHOT_DATE,
	ISONPROMO_SI,
	ISONPROMO_SO,
	FCF_TOT_VOL_KG,
	FCF_TOT_VOL_CA,
	FCF_TOT_VOL_UL,
	FCF_BASE_VOL_KG,
	FCF_BASE_VOL_CA,
	FCF_BASE_VOL_UL,
	FCF_PROMO_VOL_KG,
	FCF_PROMO_VOL_CA,
	FCF_PROMO_VOL_UL,
	ISONPROMO_FCST,
	BASE_EXT_ING_COST,
	BASE_EXT_PKG_COST,
	BASE_EXT_LBR_COST,
	BASE_EXT_BOUGHT_IN_COST,
	BASE_EXT_OTH_COST,
	BASE_EXT_COPACK_COST,
	BASE_EXT_ING_AMT,
	BASE_EXT_PKG_AMT,
	BASE_EXT_LBR_AMT,
	BASE_EXT_BOUGHT_IN_AMT,
	BASE_EXT_OTH_AMT,
	BASE_EXT_COPACK_AMT,
	REPORT_FISCAL_YEAR,
	REPORT_FISCAL_YEAR_PERIOD_NO,
	FISCAL_YEAR_BEGIN_DT,
	FISCAL_YEAR_END_DT,
	PLANNING_WEEK_CODE,
	PLANNING_WEEK_START_DT,
	PLANNING_WEEK_END_DT,
	PLANNING_WEEK_NO,
	PLANNING_MONTH_CODE,
	PLANNING_MONTH_START_DT,
	PLANNING_MONTH_END_DT,
	PLANNING_QUARTER_NO,
	PLANNING_QUARTER_START_DT,
	PLANNING_QUARTER_END_DT,
	PLANNING_YEAR_NO,
	PLANNING_YEAR_START_DT,
	PLANNING_YEAR_END_DT,
	MARKET,
	SUBMARKET,
	TRADE_CLASS,
	TRADE_GROUP,
	TRADE_TYPE,
	TRADE_SECTOR,
	DESCRIPTION,
	ITEM_TYPE,
	BRANDING_DESC,
	PRODUCT_CLASS_DESC,
	SUB_PRODUCT_DESC,
	STRATEGIC_DESC,
	POWER_BRAND_DESC,
	MANUFACTURING_GROUP_DESC,
	CATEGORY_DESC,
	PACK_SIZE_DESC,
	SUB_CATEGORY_DESC,
	PROMO_TYPE_DESC,
	DUMMY_PRODUCT_FLAG
from cte_final


