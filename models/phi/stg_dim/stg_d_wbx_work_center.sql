with T1 as(
    select * from {{ref('src_dimensionattributevaluesetitem')}}
),

T2 as(
    select * from {{ref('src_dimensionattributevalue')}}
),

DALVV as (
    SELECT T1.DIMENSIONATTRIBUTEVALUE    AS "DIMENSIONATTRIBUTEVALUE",
           T1.DIMENSIONATTRIBUTEVALUESET AS "DIMENSIONATTRIBUTEVALUESET",
           T1.DISPLAYVALUE               AS "DISPLAYVALUE",
           T1.RECID                      AS "SETITEMRECID",
           T1.PARTITION                  AS "PARTITION",
           T1.RECID                      AS "RECID",
           T2.PARTITION                  AS "PARTITION#2",
           T2.DIMENSIONATTRIBUTE         AS "DIMENSIONATTRIBUTE",
           T2.ENTITYINSTANCE             AS "ENTITYINSTANCE",
           T2.RECID                      AS "ATTRIBUTEVALUERECID"
      FROM T1
      CROSS JOIN  T2
     WHERE (    T1.DIMENSIONATTRIBUTEVALUE = T2.RECID AND (T1.PARTITION = T2.PARTITION))
),


wct as (
    select * from {{ref('src_wrkctrtable')}}
),

DA as(
    select * from {{ref('src_dimensionattribute')}}
),

DP as(
SELECT 
DISPLAYVALUE,
DALVV.DIMENSIONATTRIBUTEVALUESET FROM DALVV
INNER JOIN  DA ON DALVV.DIMENSIONATTRIBUTE = DA.RECID WHERE DA.NAME = 'Plant'
),

Final as(
select 
'{{env_var("DBT_SOURCE_SYSTEM")}}' AS SOURCE_SYSTEM
,WCT.WRKCTRID AS WORK_CENTER_CODE
,NVL(WCT.NAME,' ') AS DESCRIPTION
,DP.DISPLAYVALUE AS SOURCE_BUSINESS_UNIT_CODE
,NVL(WCT.DATAAREAID,' ') COMPANY_CODE
,NVL(WCT.WRKCTRTYPE,0)  AS WC_CATEGORY_CODE
,WCT.WRKCTRTYPE AS WC_CATEGORY_DESC,
WCT.WRKCTRTYPE AS WRKCTRTYPE
FROM  WCT
INNER JOIN DP ON WCT.DEFAULTDIMENSION = DP.DIMENSIONATTRIBUTEVALUESET
),

stg as(select 
SOURCE_SYSTEM,
WORK_CENTER_CODE,
DESCRIPTION,
SOURCE_BUSINESS_UNIT_CODE,
COMPANY_CODE,
WC_CATEGORY_CODE,
CASE
WHEN  WRKCTRTYPE= '0' THEN 'Supplier'
WHEN  WRKCTRTYPE= '1' THEN 'Human Resources'
WHEN  WRKCTRTYPE= '2' THEN 'Machine'
WHEN  WRKCTRTYPE= '3' THEN 'Tool'
WHEN  WRKCTRTYPE= '4' THEN 'Location'
WHEN  WRKCTRTYPE= '5' THEN 'Resource Group'
ELSE NULL
END as WC_CATEGORY_DESC,
WRKCTRTYPE,
systimestamp () as update_date,
ROW_NUMBER() OVER (PARTITION BY COMPANY_CODE,SOURCE_BUSINESS_UNIT_CODE,WORK_CENTER_CODE ORDER BY 1) rowNum
from final
)

select * from stg where rownum=1
