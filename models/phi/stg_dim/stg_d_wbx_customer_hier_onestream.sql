with 
CUST_EXT as 
(
    select * from {{ref('dim_wbx_customer_ext')}}
), 

/* As part of D365 this code was done in which the MDS source was dropped. MDS source was dropped as all the new customers 
would be flowing from D365 and not from MDM so we dont need MDS as a source also if we keep MDS as a source then we have duplicates which is
causing sale numbers to double */

CUST_DIM as
(
SELECT 
SOURCE_SYSTEM_ADDRESS_NUMBER,CUSTOMER_NAME,COMPANY_CODE,customer_address_number_guid
FROM {{ref('dim_wbx_customer')}} A  WHERE A.SOURCE_SYSTEM = 'WEETABIX' /*AND A.CUSTOMER_TYPE = 'CB'*/
),

cust as(
SELECT 
SOURCE_SYSTEM,
COMPANY_CODE,
HIER_CATEGORY,
HIER_NAME,
ACCOUNT_CODE,
LEAF_NODE,
NODE_LEVEL,
NODE_1,
DESC_1,
NODE_2,
DESC_2,
NODE_3,
DESC_3,
NODE_4,
DESC_4,
NODE_5,
DESC_5,
NODE_6,
DESC_6,
NODE_7,
DESC_7
FROM
(
SELECT distinct 
'{{env_var("DBT_SOURCE_SYSTEM")}}' AS SOURCE_SYSTEM,
CUST_DIM.COMPANY_CODE as COMPANY_CODE,
'CUSTOMER'                         AS HIER_CATEGORY,
'CUSTOMER-SALES'                   AS HIER_NAME,
NULL                               AS ACCOUNT_CODE,
CUST_DIM.SOURCE_SYSTEM_ADDRESS_NUMBER AS LEAF_NODE,
7                                  AS NODE_LEVEL,
CUST_EXT.MARKET_CODE                  AS NODE_1,
CUST_EXT.MARKET_DESC         AS DESC_1,
CUST_EXT.SUB_MARKET_CODE               AS NODE_2,
CUST_EXT.SUB_MARKET_DESC           AS DESC_2,
CUST_EXT.TRADE_CLASS_CODE                  AS NODE_3,
CUST_EXT.TRADE_CLASS_DESC         AS DESC_3,
CUST_EXT.TRADE_GROUP_CODE                  AS NODE_4,
CUST_EXT.TRADE_GROUP_DESC         AS DESC_4,
CUST_EXT.TRADE_TYPE_CODE                   AS NODE_5,
CUST_EXT.TRADE_TYPE_DESC           AS DESC_5,
CUST_DIM.SOURCE_SYSTEM_ADDRESS_NUMBER AS NODE_6,
CUST_DIM.CUSTOMER_NAME                AS DESC_6,
CUST_DIM.SOURCE_SYSTEM_ADDRESS_NUMBER AS NODE_7,
CUST_DIM.CUSTOMER_NAME                 AS DESC_7
FROM
CUST_EXT 
inner join 
CUST_DIM
on CUST_DIM.customer_address_number_guid=CUST_EXT.customer_address_number_guid 
)
--where rank=1 Removed this filter as we need data based on company.With this filter we were not getting data based on Company.
), 

final as (
    select * from cust
)

SELECT 
    CAST (TRIM (SOURCE_SYSTEM) AS VARCHAR2 (60)) AS SOURCE_SYSTEM,
    CAST (TRIM (COMPANY_CODE) AS VARCHAR2 (60)) AS COMPANY_CODE,
    CAST (TRIM (HIER_CATEGORY) AS VARCHAR2 (60))          AS HIER_CATEGORY,
    CAST (TRIM (HIER_NAME) AS VARCHAR2 (60))      AS HIER_NAME,
    CAST (TRIM (ACCOUNT_CODE) AS VARCHAR2 (60))  AS tagetik_account,
    CAST (TRIM (LEAF_NODE) AS VARCHAR2 (60))  AS LEAF_NODE,
    CAST (TRIM (NODE_LEVEL) AS NUMBER (15))    AS NODE_LEVEL,
    CAST (TRIM (NODE_1) AS VARCHAR2 (60))      AS NODE_1,
    CAST (TRIM (DESC_1) AS VARCHAR2 (255))     AS DESC_1,
    CAST (TRIM (NODE_2) AS VARCHAR2 (60))      AS NODE_2,
    CAST (TRIM (DESC_2) AS VARCHAR2 (255))     AS DESC_2,
    CAST (TRIM (NODE_3) AS VARCHAR2 (60))      AS NODE_3,
    CAST (TRIM (DESC_3) AS VARCHAR2 (255))     AS DESC_3,
    CAST (TRIM (NODE_4) AS VARCHAR2 (60))      AS NODE_4,
    CAST (TRIM (DESC_4) AS VARCHAR2 (255))     AS DESC_4,
    CAST (TRIM (NODE_5) AS VARCHAR2 (60))      AS NODE_5,
    CAST (TRIM (DESC_5) AS VARCHAR2 (255))     AS DESC_5,
    CAST (TRIM (NODE_6) AS VARCHAR2 (60))      AS NODE_6,
    CAST (TRIM (DESC_6) AS VARCHAR2 (255))     AS DESC_6,
    CAST (TRIM (NODE_7) AS VARCHAR2 (60))      AS NODE_7,
    CAST (TRIM (DESC_7) AS VARCHAR2 (255))     AS DESC_7,
    NULL                                       AS NODE_8,
    NULL                                       AS DESC_8,
    NULL                                      AS NODE_9,
    NULL                                      AS DESC_9,
    NULL                                      AS NODE_10,
    NULL                                      AS DESC_10,
    NULL                                      AS NODE_11,
    NULL                                      AS DESC_11,
    NULL                                      AS NODE_12,
    NULL                                      AS DESC_12,
    NULL                                      AS NODE_13,
    NULL                                      AS DESC_13,
    NULL                                      AS NODE_14,
    NULL                                      AS DESC_14,
    NULL                                      AS NODE_15,
    NULL                                      AS DESC_15,
    NULL                                      AS NODE_16,
    NULL                                      AS DESC_16,
    NULL                                      AS NODE_17,
    NULL                                      AS DESC_17,
    NULL                                      AS NODE_18,
    NULL                                      AS DESC_18,
    NULL                                      AS NODE_19,
    NULL                                      AS DESC_19,
    NULL                                      AS NODE_20,
    NULL                                      AS DESC_20,     
    CURRENT_DATE                              AS LOAD_DATE,
    CURRENT_DATE                              AS UPDATE_DATE
from Final