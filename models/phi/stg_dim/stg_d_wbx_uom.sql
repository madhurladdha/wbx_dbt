/* Mike Traub 5/29/2024
    The changes included in the pipeline for loading dim_wbx_uom, the primary UOM conversion table for Weetabix for the D365 project.
    These changes are needed due to adjustments in how the relevant Item UOM conversions and weights are stored and prioritized for reporting purposes.
    Changes included:
        -For CA to KG we want to ensure that the Net Weight from INVENTTABLE take priority.  In AX, the Net Weight was stored in whsphysdimuom but is not in D365.
        -For Pallets to KG we will use the conversion stored in the Physical Dim table (whsphysdimuom).  No longer using the Net Case Weight x Cast to Pallet number.
        -Includes logic to ensure that only 1 conversion (net weight is used for any item-company combination.)
*/

with from_uom as(
    select *
        from {{ref('src_unitofmeasure')}}
),

b as(
    select * from {{ref('src_unitofmeasureconversion')}}
),

to_uom as(
    select *
        from {{ref('src_unitofmeasure')}}
),

item as(
    select * from {{ref('src_inventtable')}} qualify row_number() over(partition by itemid order by dataareaid ASC)=1 /*added to filter as currentl there are two rows coming for same item*/
),
whsphysdimuom as (
    select * from {{ref('src_whsphysdimuom')}} qualify row_number() over(partition by itemid,uom order by dataareaid desc)=1 /*added to filter as currentl there are two rows coming for same item*/
),

exclude_in as
(
select 
    distinct
        item.itemid
        , upper (trim (from_uom.symbol))
        from 
            from_uom,
            b,
            to_uom,
            item  
        where from_uom.recid = b.fromunitofmeasure 
        and to_uom.recid = b.tounitofmeasure 
        and b.product = item.product 
        and upper (trim (to_uom.symbol)) = 'KG' 
        and  upper(item.dataareaid) in {{env_var("DBT_COMPANY_FILTER")}}
),

-------part 1 of 4 unions -- capture the conversions between non-constants.  primarily exist for ing/pkg items
union_part1 as (
    select  
           item.itemid as item_id,
           '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_indicator,
           cast (from_uom.symbol as varchar2 (255)) as from_uom,
           cast (to_uom.symbol as varchar2 (255)) as to_uom,
           b.factor as conversion_rate,
           to_decimal(1,38,10)/to_decimal(b.factor,38,10) as inversion_rate,
           to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
           to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
           'Y' as active_flag
        from from_uom,
        b,
        to_uom,
        item 
        where from_uom.recid = b.fromunitofmeasure
           and to_uom.recid = b.tounitofmeasure
           and b.product = item.product
		   and upper(item.dataareaid) in {{env_var("DBT_COMPANY_FILTER")}}
),
  -------Part 2 of 4 Unions - Capture the inverse rates as part 1.
union_part2 as
(
     select 
        item.itemid as item_id,
        '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_indicator,
        cast (to_uom.symbol as varchar2 (255)) as from_uom,
        cast (from_uom.symbol as varchar2 (255)) as to_uom,
        to_decimal(1,38,10)/to_decimal(b.factor,38,10) as conversion_rate,
        b.factor as inversion_rate,
        to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
        to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
        'Y' as active_flag
      from from_uom,
           b,
           to_uom,
           item 
           where from_uom.recid = b.fromunitofmeasure 
           and to_uom.recid = b.tounitofmeasure
           and b.product = item.product 
           and upper(item.dataareaid) in {{env_var("DBT_COMPANY_FILTER")}}
),

---Capture the conversions between to KG.  Primarily exist for Finished Goods Items.
---Weetabix gave direction that could assume that weight values are all known to be in KG.

UNION_PART3 as
(
SELECT 
        A.ITEMID                                   AS ITEMID,
        '{{env_var("DBT_SOURCE_SYSTEM")}}'         AS SOURCE_INDICATOR,
        CAST (A.UOM AS VARCHAR2 (255))             AS FROM_UOM,
        CAST ('KG' AS VARCHAR2 (255))              AS TO_UOM,
        CAST (A.WEIGHT AS NUMBER (38, 10))         AS CONVERSION_RATE,
        to_decimal(1,38,10)/to_decimal(A.WEIGHT,38,10)     AS INVERSION_RATE,
        to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
        to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
        'Y' as active_flag
      FROM WHSPHYSDIMUOM A  
      WHERE 
        A.WEIGHT > 0 
        AND UPPER(A.DATAAREAID) in {{env_var("DBT_COMPANY_FILTER")}}
        AND (A.ITEMID, UPPER (TRIM (A.UOM))) NOT IN (select * from Exclude_in)
                   
),

---Capture the inverse rates as part 3.
UNION_PART4 as (
SELECT 
        B.ITEMID                                   AS ITEMID,
        '{{env_var("DBT_SOURCE_SYSTEM")}}'         AS SOURCE_INDICATOR,
        CAST ('KG' AS VARCHAR2 (255))              AS FROM_UOM,
        CAST (B.UOM AS VARCHAR2 (255))             AS TO_UOM,
        CAST (1 / B.WEIGHT AS NUMBER (38, 10))     AS CONVERSION_RATE,
        to_decimal(1,38,10)/to_decimal(B.WEIGHT,38,10)         AS INVERSION_RATE,
        to_date('01-JAN-1980', 'DD-MON-YYYY') as effective_date,
        to_date('31-DEC-2040', 'DD-MON-YYYY') as expiration_date,
        'Y' as active_flag
      FROM WHSPHYSDIMUOM B
      WHERE 
        B.WEIGHT > 0 
        AND UPPER(B.DATAAREAID) in {{env_var("DBT_COMPANY_FILTER")}}
        AND (B.ITEMID, UPPER (TRIM (B.UOM))) NOT IN (SELECT * from Exclude_in)
),

final as (
    select * from union_part1
    union all
    select * from union_part2
 
    union all
    select * from union_part3
    union all
    select * from union_part4
    
)


select * from final
