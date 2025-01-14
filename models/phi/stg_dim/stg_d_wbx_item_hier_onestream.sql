with 

/* As part of D365 code changes this was updated as from MDS the new item wont be entered and due to this the new 
items which are entered in IBE are not getting fetched in the final Product view.Adding this to derive from Item ext*/

item_ext as 
(
    select distinct SOURCE_ITEM_IDENTIFIER,
                    BRANDING_CODE,
                    BRANDING_DESC,
                    PRODUCT_CLASS_CODE,
                    PRODUCT_CLASS_DESC,
                    SUB_PRODUCT_DESC,
                    SUB_PRODUCT_CODE,
                    DESCRIPTION
     from {{ref('dim_wbx_item_ext')}} qualify row_number() over(partition by SOURCE_ITEM_IDENTIFIER order by date_inserted desc)=1 
     /*two different descriptions are coming for same item from upstream .added qualify filter to pick latest description for an item */
),
/*
union1 as(

SELECT '{{env_var("DBT_SOURCE_SYSTEM")}}' AS SOURCE_SYSTEM,
'ITEM' AS HIER_CATEGORY,
'ITEM-SALES' AS HIER_NAME,
NULL AS ACCOUNT_CODE,
MDS_IM.CODE AS LEAF_NODE,
4  AS NODE_LEVEL,
MDS_IM.PRODUCTBRANDING  AS NODE_1,
MDS_IM.PRODUCTBRANDING_DESC AS DESC_1,
MDS_IM.PRODUCTCLASS AS NODE_2,
MDS_IM.PRODUCTCLASS_DESC AS DESC_2,
MDS_IM.PRODUCTSUBPRODUCT AS NODE_3,
MDS_IM.PRODUCTSUBPRODUCT_DESC DESC_3,
MDS_IM.CODE AS NODE_4,
MDS_IM.NAME AS DESC_4
FROM
MDS_IM
), */

union1 as(

SELECT '{{env_var("DBT_SOURCE_SYSTEM")}}' AS SOURCE_SYSTEM,
'ITEM' AS HIER_CATEGORY,
'ITEM-SALES' AS HIER_NAME,
NULL AS ACCOUNT_CODE,
item_ext.SOURCE_ITEM_IDENTIFIER AS LEAF_NODE,
4  AS NODE_LEVEL,
item_ext.BRANDING_CODE  AS NODE_1,
item_ext.BRANDING_DESC AS DESC_1,
item_ext.PRODUCT_CLASS_CODE AS NODE_2,
item_ext.PRODUCT_CLASS_DESC AS DESC_2,
item_ext.SUB_PRODUCT_CODE AS NODE_3,
item_ext.SUB_PRODUCT_DESC DESC_3,
item_ext.SOURCE_ITEM_IDENTIFIER AS NODE_4,
item_ext.DESCRIPTION AS DESC_4
FROM
item_ext
)

SELECT CAST (TRIM (SOURCE_SYSTEM) AS VARCHAR2 (60)) AS SOURCE_SYSTEM,
    CAST (TRIM (HIER_CATEGORY) AS VARCHAR2 (60))          AS HIER_CATEGORY,
    CAST (TRIM (HIER_NAME) AS VARCHAR2 (60))      AS HIER_NAME,
    CAST (TRIM (ACCOUNT_CODE) AS VARCHAR2 (60))  AS tagetik_account,
    CAST (TRIM (LEAF_NODE) AS VARCHAR2 (60))  AS LEAF_NODE,
    CAST (TRIM (NODE_LEVEL) AS NUMBER (15))    AS NODE_LEVEL,
    CAST (TRIM (NODE_1) AS VARCHAR2 (60))     AS NODE_1,
    CAST (TRIM (DESC_1) AS VARCHAR2 (255))  AS DESC_1,
    CAST (TRIM (NODE_2) AS VARCHAR2 (60))     AS NODE_2,
    CAST (TRIM (DESC_2) AS VARCHAR2 (255))  AS DESC_2,
    CAST (TRIM (NODE_3) AS VARCHAR2 (60))     AS NODE_3,
    CAST (TRIM (DESC_3) AS VARCHAR2 (255))  AS DESC_3,
    CAST (TRIM (NODE_4) AS VARCHAR2 (60))     AS NODE_4,
    CAST (TRIM (DESC_4) AS VARCHAR2 (255))  AS DESC_4,
    NULL AS NODE_5,
NULL AS DESC_5,
NULL AS NODE_6,
NULL AS DESC_6,
NULL AS NODE_7,
NULL AS DESC_7,
NULL AS NODE_8,
NULL AS DESC_8,
NULL AS NODE_9,
NULL AS DESC_9,
NULL AS NODE_10,
NULL AS DESC_10,
NULL AS NODE_11,
NULL AS DESC_11,
NULL AS NODE_12,
NULL AS DESC_12,
NULL AS NODE_13,
NULL AS DESC_13,
NULL AS NODE_14,
NULL AS DESC_14,
NULL AS NODE_15,
NULL AS DESC_15,
NULL AS NODE_16,
NULL AS DESC_16,
NULL AS NODE_17,
NULL AS DESC_17,
NULL AS NODE_18,
NULL AS DESC_18,
NULL AS NODE_19,
NULL AS DESC_19,
NULL AS NODE_20,
NULL AS DESC_20, 
CURRENT_DATE AS LOAD_DATE,
CURRENT_DATE AS UPDATE_DATE
from union1