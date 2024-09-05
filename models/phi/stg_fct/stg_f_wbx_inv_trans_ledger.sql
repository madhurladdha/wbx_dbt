{{ config(tags=["inventory", "trans_ledger"],snowflake_warehouse= env_var("DBT_WBX_SF_WH") ) }}

/* The fact refresh has to be a FULL REFRESH (not incremental) due to the complexities around the natural key and unique key.  There are risks of breaking the 
    uniqueness and either overcounting or overwriting rows.  An incremental approach may be feasible if the filter is applied to the correct table and tested,
    but the risk is not being taken at this time. 
*/

with
    inventtrans as (select * from {{ ref("src_inventtrans") }}),

    inventtransorigin as (select * from {{ ref("src_inventtransorigin") }}),

    inventdim as (select * from {{ ref("src_inventdim") }}),
    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),
    inventsum as (select * from {{ ref("src_inventsum") }}),
    wmslocation as (select * from {{ ref("src_wmslocation") }}),
    salesline as (select * from {{ ref("src_salesline") }}),
    purchline as (select * from {{ ref("src_purchline") }}),
    inventjournaltrans as (select * from {{ ref("src_inventjournaltrans") }}),
    whsworkinventtrans as (select * from {{ ref("src_whsworkinventtrans") }}),
    whsworkquarantine as (select * from {{ ref("src_whsworkquarantine") }}),

    

    src_table as 
    (SELECT 
'{{ env_var("DBT_SOURCE_SYSTEM") }}' AS SOURCE_SYSTEM ,
  ITO.REFERENCECATEGORY
  || '.'
  || IT.STATUSISSUE
  || '.'
  || IT.STATUSRECEIPT                                                           AS SOURCE_DOCUMENT_TYPE ,
  cast(NULL as string(255))                                                                          AS SOURCE_ORIGINAL_DOCUMENT_TYPE ,
  cast(NULL as string(255))                                                                           AS RELATED_ADDRESS_NUMBER ,
  IT.DATEFINANCIAL                                                              AS GL_DATE ,
  ITO.INVENTTRANSID                                                             AS DOCUMENT_NUMBER ,
  ORIG.ORIG_DOC_NUM                                                             AS ORIGINAL_DOCUMENT_NUMBER ,
  IT.ITEMID                                                                     AS SOURCE_ITEM_IDENTIFIER ,
  TRIM(UPPER(IT.DATAAREAID))                                                    AS DOCUMENT_COMPANY ,
  ORIG.ORIG_DOC_CO                                                              AS ORIGINAL_DOCUMENT_COMPANY ,
  ROW_NUMBER() OVER (PARTITION BY ITO.INVENTTRANSID ORDER BY ITO.INVENTTRANSID) AS LINE_NUMBER ,
  ORIG.ORIG_LINE_NUM                                                            AS ORIGINAL_LINE_NUMBER ,
  CASE WHEN ID.WMSLOCATIONID IS NULL OR ID.WMSLOCATIONID = '' THEN '-' ELSE ID.WMSLOCATIONID END AS SOURCE_LOCATION_CODE ,
  CASE WHEN ID.INVENTBATCHID IS NULL OR ID.INVENTBATCHID = '' THEN '-' ELSE ID.INVENTBATCHID END AS SOURCE_LOT_CODE ,
  cast(NULL as string(255))                                                                           AS LOT_STATUS_CODE ,
  ID.INVENTLOCATIONID                                                           AS SOURCE_BUSINESS_UNIT_CODE ,
  SUM( CAST(IT.COSTAMOUNTPOSTED AS NUMBER(27,9)) )                              AS TRANSACTION_AMT ,
  cast(NULL as string(255))                                                                           AS REASON_CODE ,
  IT.DATEPHYSICAL                                                               AS TRANSACTION_DATE ,
  cast(NULL as string(255))                                                                           AS REMARK_TXT ,
  SUM( CAST(IT.QTY AS NUMBER(27,9)) )                                           AS TRANSACTION_QTY ,
  ITM.UNITID                                                                    AS TRANSACTION_UOM 
,cast(SUM(round(IT.COSTAMOUNTPOSTED,9)/round(IT.QTY,9)) as number(27,9)) AS TRANSACTION_UNIT_COST,
  IT.CURRENCYCODE                                                               AS TRANSACTION_CURRENCY ,
  IT.MODIFIEDDATETIME                                                           AS SOURCE_UPDATE_DATE,
  ID.LICENSEPLATEID                                                             AS SOURCE_PALLET_ID,
 case when ID.INVENTSIZEID ='' then ' ' else ID.INVENTSIZEID end AS VARIANT,
  CASE
    WHEN LOC.LOCPROFILEID IN ('Non LP','Silo','SILOS')
    THEN 0
    WHEN (INS.PhysicalInvent = 0
    AND INS.OnOrder          = 0)
    THEN 0
    WHEN IT.QTY < 0
    THEN -1
    ELSE 1
  END AS PALLET_COUNT
FROM inventtrans IT
INNER JOIN inventtransorigin ITO
ON IT.INVENTTRANSORIGIN = ITO.RECID
INNER JOIN inventdim ID
ON IT.INVENTDIMID = ID.INVENTDIMID
LEFT OUTER JOIN
  (SELECT * FROM inventtablemodule WHERE MODULETYPE = 0
  ) ITM
ON TRIM(UPPER(IT.DATAAREAID)) = TRIM(UPPER(ITM.DATAAREAID))
AND IT.ITEMID                 = ITM.ITEMID
LEFT OUTER JOIN inventsum INS
ON TRIM(UPPER(IT.DATAAREAID)) = TRIM(UPPER(INS.DATAAREAID))
AND IT.ITEMID                 = INS.ITEMID
AND IT.INVENTDIMID            = INS.INVENTDIMID
LEFT OUTER JOIN wmslocation LOC
ON TRIM(UPPER(ID.DATAAREAID))        = TRIM(UPPER(LOC.DATAAREAID))
AND TRIM(UPPER(ID.INVENTLOCATIONID)) = TRIM(UPPER(LOC.INVENTLOCATIONID))
AND ID.WMSLOCATIONID                 = LOC.WMSLOCATIONID
LEFT OUTER JOIN
  (SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    SALESID                       AS ORIG_DOC_NUM,
    LINENUM                       AS ORIG_LINE_NUM,
    INVENTTRANSID
  FROM salesline
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    PURCHID                      AS ORIG_DOC_NUM,
    LINENUMBER                   AS ORIG_LINE_NUM,
    INVENTTRANSID
  FROM purchline
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    JOURNALID                    AS ORIG_DOC_NUM,
    LINENUM                      AS ORIG_LINE_NUM,
    INVENTTRANSID
  FROM inventjournaltrans
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    WORKID                       AS ORIG_DOC_NUM,
    LINENUM                      AS ORIG_LINE_NUM,
    INVENTTRANSIDFROM            AS INVENTTRANSID
  FROM whsworkinventtrans
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    WORKID                       AS ORIG_DOC_NUM,
    LINENUM                      AS ORIG_LINE_NUM,
    INVENTTRANSIDTO              AS INVENTTRANSID
  FROM whsworkinventtrans
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    WORKID                       AS ORIG_DOC_NUM,
    LINENUM                      AS ORIG_LINE_NUM,
    INVENTTRANSIDFROM            AS INVENTTRANSID
  FROM whsworkquarantine
  UNION
  SELECT UPPER(TRIM(DATAAREAID)) AS ORIG_DOC_CO,
    WORKID                       AS ORIG_DOC_NUM,
    LINENUM                      AS ORIG_LINE_NUM,
    INVENTTRANSIDTO              AS INVENTTRANSID
  FROM whsworkquarantine
  ) ORIG
ON ITO.INVENTTRANSID                          = ORIG.INVENTTRANSID
WHERE IT.QTY                                 <> 0
--AND DATEDIFF(day, to_date(IT.MODIFIEDDATETIME), current_date) <=3
--AND DATEDIFF(day, to_date(IT.DATEPHYSICAL), current_date) <=3
GROUP BY ITO.INVENTTRANSID,
  IT.ITEMID,
  ITO.REFERENCECATEGORY,
  IT.STATUSISSUE,
  IT.STATUSRECEIPT,
  IT.DATAAREAID ,
  IT.CURRENCYCODE,
  IT.DATEPHYSICAL,
  IT.DATEFINANCIAL,
  ID.INVENTSITEID,
  ID.INVENTLOCATIONID,
  ID.WMSLOCATIONID,
  ID.INVENTBATCHID,
  ITM.UNITID,
  IT.MODIFIEDDATETIME ,
  ORIG.ORIG_DOC_NUM,
  ORIG.ORIG_DOC_CO,
  ORIG.ORIG_LINE_NUM,
  ID.LICENSEPLATEID,
  ID.INVENTSIZEID,
  CASE
    WHEN LOC.LOCPROFILEID IN ('Non LP','Silo','SILOS')
    THEN 0
    WHEN (INS.PhysicalInvent = 0
    AND INS.OnOrder          = 0)
    THEN 0
    WHEN IT.QTY < 0
    THEN -1
    ELSE 1
  END

    )

select  cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_document_type,1,20) as text(20) ) as source_document_type  ,

    cast(substring(source_original_document_type,1,20) as text(20) ) as source_original_document_type  ,

    cast(substring(related_address_number,1,255) as text(255) ) as related_address_number  ,

    cast(gl_date as timestamp_ntz(9) ) as gl_date  ,

    cast(substring(document_number,1,255) as text(255) ) as document_number  ,

    cast(substring(original_document_number,1,255) as text(255) ) as original_document_number  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(substring(document_company,1,20) as text(20) ) as document_company  ,

    cast(substring(original_document_company,1,20) as text(20) ) as original_document_company  ,

    cast(line_number as number(38,10) ) as line_number  ,

    cast(original_line_number as number(38,10) ) as original_line_number  ,

    cast(substring(source_location_code,1,255) as text(255) ) as source_location_code  ,

    cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,

    cast(substring(lot_status_code,1,255) as text(255) ) as lot_status_code  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(transaction_amt as number(27,9) ) as transaction_amt  ,

    cast(substring(reason_code,1,255) as text(255) ) as reason_code  ,

    cast(transaction_date as timestamp_ntz(9) ) as transaction_date  ,

    cast(substring(remark_txt,1,255) as text(255) ) as remark_txt  ,

    cast(transaction_qty as number(27,9) ) as transaction_qty  ,

    cast(substring(transaction_uom,1,20) as text(20) ) as transaction_uom  ,

    cast(transaction_unit_cost as number(27,9) ) as transaction_unit_cost  ,

    cast(substring(transaction_currency,1,20) as text(20) ) as transaction_currency  ,

    cast(source_update_date as timestamp_ntz(9) ) as source_update_date  ,

    cast(substring(source_pallet_id,1,255) as text(255) ) as source_pallet_id  ,

    cast(substring(variant,1,255) as text(255) ) as variant  ,

    cast(pallet_count as number(15,2) ) as pallet_count 
from src_table    
