/* Mike Traub 5/29/2024
    The changes included in the pipeline for loading dim_wbx_uom, the primary UOM conversion table for Weetabix for the D365 project.
    These changes are needed due to adjustments in how the relevant Item UOM conversions and weights are stored and prioritized for reporting purposes.
    Changes included:
        -For CA to KG we want to ensure that the Net Weight from INVENTTABLE take priority.  In AX, the Net Weight was stored in whsphysdimuom but is not in D365.
        -For Pallets to KG we will use the conversion stored in the Physical Dim table (whsphysdimuom).  No longer using the Net Case Weight x Cast to Pallet number.
        -Includes logic to ensure that only 1 conversion (net weight is used for any item-company combination.)
*/

with ITEM as(
    select * from {{ref('src_inventtable')}} qualify row_number() over(partition by itemid order by dataareaid desc)=1 /*added to filter as currentl there are two rows coming for same item*/
),

----Using the item extention to get the Pallet Qty value which provides the Cases per Pallet number needed. 
IT_EXT as
(
    select * from {{ref('stg_d_wbx_item_ext')}}
),

-------Part 1 of 4: Capture CA->KG from Item Master itself
UNION_PART1 as (
    SELECT  ITEM.ITEMID                  AS ITEM_ID,
           '{{env_var("DBT_SOURCE_SYSTEM")}}'  AS SOURCE_INDICATOR,
           CAST ('Case' AS VARCHAR2 (255))     AS  FROM_UOM,
           CAST ('KG' AS VARCHAR2 (255))       AS TO_UOM,
           ITEM.NETWEIGHT                     AS CONVERSION_RATE,
           CASE WHEN ITEM.NETWEIGHT=0 THEN 0 ELSE TO_DECIMAL(1 / ITEM.NETWEIGHT,38,10) END AS INVERSION_RATE,
    to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
    to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
    'Y' as active_flag
      FROM ITEM WHERE TRIM(UPPER(ITEM.DATAAREAID)) in {{env_var("DBT_COMPANY_FILTER")}}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid,dataareaid ORDER BY modifieddatetime desc) = 1
       ),

 -------Part 2 of 4: Capture KG->CA from Item Master itself
UNION_PART2 as (
SELECT  
    ITEM.ITEMID AS ITEM_ID,
    '{{env_var("DBT_SOURCE_SYSTEM")}}' AS SOURCE_INDICATOR,
    CAST ('KG' AS VARCHAR2 (255)) AS FROM_UOM,
    CAST ('Case' AS VARCHAR2 (255)) AS TO_UOM,
    CASE WHEN ITEM.NETWEIGHT=0 THEN 0 ELSE TO_DECIMAL(1 / ITEM.NETWEIGHT,38,10) END AS CONVERSION_RATE,
    ITEM.NETWEIGHT AS INVERSION_RATE,
    to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
    to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
    'Y' as active_flag
from item 
where trim(upper(item.dataareaid)) in {{env_var("DBT_COMPANY_FILTER")}} 
qualify row_number() over (partition by itemid,dataareaid order by modifieddatetime desc) = 1
),

/* Part 3 and Part 4 are for Pallet to KG conversions.  But for now the requirement is to continue getting that conversion
    from the Physical Dimensions table (whsphysdimuom) referenced in the stg1 of this UOM conversion logic.
    The logic is remaining for now but will be excluded now from the UNION down below of the 4 parts.
*/

UNION_PART3 as (
SELECT  ITEM.ITEMID                                  AS ITEM_ID,
'{{env_var("DBT_SOURCE_SYSTEM")}}'                                    AS SOURCE_INDICATOR,
 CAST ('PL' AS VARCHAR2 (255))     AS FROM_UOM,
 CAST ('KG' AS VARCHAR2 (255))       AS TO_UOM,
ITEM.NETWEIGHT * IT_EXT.PALLET_QTY AS CONVERSION_RATE,
CASE WHEN ITEM.NETWEIGHT * IT_EXT.PALLET_QTY=0 THEN 0 ELSE TO_DECIMAL(1.00 / (ITEM.NETWEIGHT * IT_EXT.PALLET_QTY),38,10) END AS INVERSION_RATE,
    to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
    to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
    'Y' as active_flag
FROM  ITEM
INNER JOIN  IT_EXT ON ITEM.ITEMID = IT_EXT.source_item_identifier /*AND ITEM.DATAAREAID = IT_EXT.DATAAREAID*/ 
WHERE TRIM(UPPER(ITEM.DATAAREAID)) in {{env_var("DBT_COMPANY_FILTER")}} 
QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid,dataareaid ORDER BY modifieddatetime desc) = 1

),

-------Part 4 of 4: Capture KG->PL from Item and WBX Item Extension
UNION_PART4 as (
SELECT  ITEM.ITEMID                                  AS ITEM_ID,
'{{env_var("DBT_SOURCE_SYSTEM")}}'                                    AS SOURCE_INDICATOR,
 CAST ('KG' AS VARCHAR2 (255))     AS FROM_UOM,
CAST ('PL' AS VARCHAR2 (255))       AS TO_UOM,
CASE WHEN ITEM.NETWEIGHT * IT_EXT.PALLET_QTY=0 THEN 0 ELSE TO_DECIMAL(1.00 / (ITEM.NETWEIGHT * IT_EXT.PALLET_QTY),38,10) END AS CONVERSION_RATE,
ITEM.NETWEIGHT * IT_EXT.PALLET_QTY AS INVERSION_RATE,
    to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
    to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
    'Y' as active_flag
FROM  ITEM
INNER JOIN IT_EXT ON ITEM.ITEMID = IT_EXT.source_item_identifier /*AND ITEM.DATAAREAID = IT_EXT.DATAAREAID*/ --Avinash 2024-05-29 commented out join condition on dataareaid as we don't have that field  in it_ext stage
WHERE TRIM(UPPER(ITEM.DATAAREAID)) in {{env_var("DBT_COMPANY_FILTER")}} 
QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid,dataareaid ORDER BY modifieddatetime desc) = 1
),


/* Part 3 and Part 4 are for Pallet to KG conversions.  But for now the requirement is to continue getting that conversion
    from the Physical Dimensions table (whsphysdimuom) referenced in the stg1 of this UOM conversion logic.
    The logic is remaining for now but will be excluded now from the UNION down below of the 4 parts.
*/
final as (
    select * from union_part1
    union all
    select * from union_part2
    -- union all
    -- select * from union_part3
    -- union all
    -- select * from union_part4
   
)


select * from final
