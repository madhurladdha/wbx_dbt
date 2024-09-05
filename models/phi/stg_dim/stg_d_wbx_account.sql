with MA1 as(
    select * from {{ref('src_mainaccount')}}
),

ledger as(
    select * from {{ref('src_ledger')}}
),

MAC as(
    select * from {{ref('src_mainaccountcategory')}}
),

DAVC as (
    select * from {{ref('src_dimensionattributevaluecombo')}}
),

DFLR as (
    select * from {{ref('src_dimensionfocusledgerdimref')}}
),

DH as
(
    select * from {{ref('src_dimensionhierarchy')}}
),

/*  For D365 changing the name referenced from MA_CC to MA+CC.  Also changing the separator from '-' to '~' for D365.
    This section is capturing the Main Account and Cost Center combinations which are the 2 main account related fields used in our account dimension.
    For D365 this is driven by the Financial Dimensions as assigned to the given Main Account values.
*/

CC as(
SELECT DISTINCT
MA1.MAINACCOUNTID AS SOURCE_ACCOUNT,
CASE WHEN UPPER (TRIM (DH.NAME)) = 'MA+CC'
THEN SUBSTR (DAVC.DISPLAYVALUE,CHARINDEX ('~', DAVC.DISPLAYVALUE) + 1)
ELSE DAVC.DISPLAYVALUE END AS COST_CENTER 
FROM MA1 
INNER JOIN DAVC ON DAVC.DISPLAYVALUE LIKE MA1.MAINACCOUNTID || '%'
INNER JOIN DFLR ON DFLR.FOCUSLEDGERDIMENSION = DAVC.RECID
INNER JOIN DH ON DFLR.FOCUSDIMENSIONHIERARCHY = DH.RECID
WHERE TRIM (UPPER (DH.NAME)) = 'MA+CC'
),

STG as(
SELECT DISTINCT
 '{{env_var("DBT_SOURCE_SYSTEM")}}'         AS SOURCE_SYSTEM,
 CAST (TRIM (LEDGER.NAME)|| '.'|| COALESCE (NULLIF(TRIM(COST_CENTER),''), '-') || '.'|| TRIM (MA1.MAINACCOUNTID) AS VARCHAR2 (60)) AS SOURCE_CONCAT_NAT_KEY,
CAST (TRIM (MA1.RECID) AS VARCHAR2 (60)) AS SOURCE_ACCOUNT_IDENTIFIER,
CAST (TRIM (MA1.MAINACCOUNTID) AS VARCHAR2 (60)) AS SOURCE_OBJECT_ID,
CAST (NULL AS VARCHAR2 (60))            AS SOURCE_SUBSIDIARY_ID,
CAST (TRIM (LEDGER.NAME) AS VARCHAR2 (60))   AS SOURCE_COMPANY_CODE,
CAST (COALESCE (NULLIF(TRIM(COST_CENTER),''), '-')  AS VARCHAR2 (60)) AS SOURCE_BUSINESS_UNIT_CODE,
CAST (TRIM (MA1.TYPE) AS VARCHAR2 (60)) AS ACCOUNT_TYPE,
CAST (TRIM (MA1.NAME) AS VARCHAR2 (60)) AS ACCOUNT_DESCRIPTION,
CAST (NULL AS VARCHAR2 (60))            AS ACCOUNT_LEVEL,
CAST (NULL AS VARCHAR2 (60))            AS ENTRY_ALLOWED_FLAG,
CAST (TRIM (MAC.ACCOUNTCATEGORY) AS VARCHAR2 (60)) AS ACCOUNT_CATEGORY,
CAST (NULL AS VARCHAR2 (60))            AS ACCOUNT_ROLLUP,
CAST (NULL AS VARCHAR2 (60))            AS ACCOUNT_SUBCATEGORY,
CAST (NULL AS VARCHAR2 (60))            AS STAT_UOM,
CAST (CURRENT_TIMESTAMP AS TIMESTAMP (6)) AS LOAD_DATE,
CAST (NULL AS TIMESTAMP (6))            AS DATE_UPDATED
From MA1
LEFT JOIN LEDGER ON MA1.LEDGERCHARTOFACCOUNTS = LEDGER.CHARTOFACCOUNTS
LEFT JOIN MAC ON MA1.ACCOUNTCATEGORYREF = MAC.ACCOUNTCATEGORYREF  and ma1.source = mac.source
LEFT JOIN CC ON MA1.MAINACCOUNTID = CC.SOURCE_ACCOUNT
),

Final as(
    select * from STG
)


select * from final