with IT as (
    select * from {{ref('src_inventtable')}}
),

ERP as (
    select * from {{ref('src_ecoresproduct')}}
),

ERPIV as (
    select * from {{ref('src_ecoresproductinstancevalue')}}
),

ERAV as (
    select * from {{ref('src_ecoresattributevalue')}}
),

ERA as (
    select * from {{ref('src_ecoresattribute')}}
),

ERTV as (
    select * from {{ref('src_ecorestextvalue')}}
),

ERIV as (
    select * from {{ref('src_ecoresintvalue')}}
),

ERBV as (
    select * from {{ref('src_ecoresbooleanvalue')}}
),

ERFV as (
    select * from {{ref('src_ecoresfloatvalue')}}
),

/*commenting below eco res cte's as it's not being
 refered by any attribute from attribute table */

/*
ERDTV as(
    select * from {{ref('src_ecoresdatetimevalue')}}
),


ERRV as(
    select * from {{ref('src_ecoresreferencevalue')}}
),
*/

/*IIGI as (
    select * from {{ref('src_inventitemgroupitem')}}
), */

DAV as (
    select * from {{ref('src_dimensionattributevalue')}}
),

DAVS as (
    select * from {{ref('src_dimensionattributevaluesetitem')}}
),

DA as (
    select * from {{ref('src_dimensionattribute')}}
),


CC as (
    select
        DAVS.PARTITION,
        DAVS.DIMENSIONATTRIBUTEVALUESET,
        DAVS.DISPLAYVALUE,
        DA.NAME
    from DAVS  -- (DEFAULTDIMENSION field's value above)
    inner join
        DAV
        on
            DAVS.PARTITION = DAV.PARTITION
            and DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
    inner join
        DA
        on DAV.PARTITION = DA.PARTITION and DAV.DIMENSIONATTRIBUTE = DA.RECID
    where DA.NAME in ('CostCenters')
),

SI as (
    select
        DAVS.PARTITION,
        DAVS.DIMENSIONATTRIBUTEVALUESET,
        DAVS.DISPLAYVALUE,
        DA.NAME
    from DAVS  -- (DEFAULTDIMENSION field's value above)
    inner join
        DAV
        on
            DAVS.PARTITION = DAV.PARTITION
            and DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
    inner join
        DA
        on DAV.PARTITION = DA.PARTITION and DAV.DIMENSIONATTRIBUTE = DA.RECID
    where DA.NAME in ('Sites')
),


DP as (
    select
        DAVS.PARTITION,
        DAVS.DIMENSIONATTRIBUTEVALUESET,
        DAVS.DISPLAYVALUE,
        DA.NAME
    from DAVS  -- (DEFAULTDIMENSION field's value above)
    inner join
        DAV
        on
            DAVS.PARTITION = DAV.PARTITION
            and DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
    inner join
        DA
        on DAV.PARTITION = DA.PARTITION and DAV.DIMENSIONATTRIBUTE = DA.RECID
    where DA.NAME in ('Department')
),

FP as (
    select
        DAVS.PARTITION,
        DAVS.DIMENSIONATTRIBUTEVALUESET,
        DAVS.DISPLAYVALUE,
        DA.NAME
    from DAVS  -- (DEFAULTDIMENSION field's value above)
    inner join
        DAV
        on
            DAVS.PARTITION = DAV.PARTITION
            and DAVS.DIMENSIONATTRIBUTEVALUE = DAV.RECID
    inner join
        DA
        on DAV.PARTITION = DA.PARTITION and DAV.DIMENSIONATTRIBUTE = DA.RECID
    where DA.NAME in ('ProductClass')
),

IM as (
    select * from {{ref('dim_wbx_item')}} where (ITEM_TYPE = 'FINISHED GOOD' OR SOURCE_ITEM_IDENTIFIER='Z000585')-- Add the Z item to list as per request from Dave.In PL report the DESCRIPTION was empty and due to this it was not showing in PL report
),

AX_IM as (
    select
        IT.ITEMID,
        IT.INVENTSTRATEGICCODEID,
        IT.INVENTPACKSIZECODEID,
        IT.NETWEIGHT,
        IT.TARAWEIGHT,
        IT.AVPWEIGHT,
        IT.AVPFLAG,
        IT.NETWEIGHT + IT.TARAWEIGHT as GROSSWEIGHT,
        IT.GROSSDEPTH,
        IT.GROSSWIDTH,
        IT.GROSSHEIGHT,
        IT.PMPFLAG,
        IT.CONSUMERUNIT,
        IT.PARTITION,
        IT.DEFAULTDIMENSION,
        IT.PDSSHELFLIFE,
    from IT
/*
  Removing this inner join as the data is not all set in D365
  and as few items are not flowing we are removing this
  inner join IIGI
        on
            UPPER(TRIM(IT.DATAAREAID)) = UPPER(TRIM(IIGI.ITEMDATAAREAID))
            and TO_CHAR(IT.ITEMID) = TO_CHAR(IIGI.ITEMID)
--Filter commented out by Avinash as no it's no longer needed
WHERE TRIM(UPPER(IT.DATAAREAID)) = 'WBX'*/
),
--WHERE ITEMGROUPID = 'FINGOODS'  ---Retrieve just Finished Goods

WHSINVENTTABLE as (
    select
        ITEMID,
        MAX(FILTERCODE) as FILTERCODE,
        MAX(FILTERCODE2_) as FILTERCODE2_,
        MAX(FILTERCODE3_) as FILTERCODE3_,
        MAX(FILTERCODE4_) as FILTERCODE4_
    from {{ref('src_whsinventtable')}} group by ITEMID
),

D365_IM as (
    select
        ITEMID as ITEM_ID,
        MAX(case when NAME = 'Manufacturing group' then TEXT_VALUE end)
            as MANUFACTURING_GROUP,
        MAX(case when NAME = 'Sub product' then TEXT_VALUE end) as SUB_PRODUCT,
        MAX(case when NAME = 'Product Class' then TEXT_VALUE end)
            as PRODUCT_CLASS,
        MAX(
            case when NAME = 'Consumer units in traded unit' then INT_VALUE end
        ) as CONSUMER_UNITS_IN_TRADED_UNIT,
        MAX(
            case
                when NAME = 'Weetabix pallet quantity per layer' then INT_VALUE
            end
        ) as WEETABIX_PALLET_QUANTITY_PER_LAYER,
        MAX(case when NAME = 'Consumer units' then INT_VALUE end)
            as CONSUMER_UNITS,
        CAST(
            MAX(case when NAME = 'AVP flag' then BOOLEAN_VALUE end) as text(255)
        ) as AVP_FLAG,
        MAX(case when NAME = 'Pallet type' then TEXT_VALUE end) as PALLET_TYPE,
        MAX(case when NAME = 'Category' then TEXT_VALUE end) as CATEGORY,
        MAX(case when NAME = 'Pack size' then TEXT_VALUE end) as PACK_SIZE,
        MAX(case when NAME = 'Powerbrand' then TEXT_VALUE end) as POWERBRAND,
        MAX(case when NAME = 'Sub-category' then TEXT_VALUE end)
            as SUB_CATEGORY,
        /* MAX(case when NAME = 'Forecast stop date' then DATE_TIME_VALUE end)
            ---commenting out as it is not being used in downstream cte
            as FORECAST_STOP_DATE,*/
        CAST(
            MAX(
                case when NAME = 'Current flag' then BOOLEAN_VALUE end
            ) as text(255)
        )
            as CURRENT_FLAG,
        MAX(case when NAME = 'Branding' then TEXT_VALUE end) as BRANDING,
        MAX(case when NAME = 'Weetabix pallet quantity' then INT_VALUE end)
            as WEETABIX_PALLET_QUANTITY,
        MAX(case when NAME = 'AVP weight' then FLOAT_VALUE end) as AVP_WEIGHT,
        MAX(case when NAME = 'Strategic' then TEXT_VALUE end) as STRATEGIC,
        MAX(case when NAME = 'Pallet configuration' then TEXT_VALUE end)
            as PALLET_CONFIGURATION,
        MAX(case when NAME = 'PMP flag' then BOOLEAN_VALUE end) as PMP_FLAG
    from IT as INV_TBL                                         --MAIN ITEM TABLE
    inner join ERP on INV_TBL.PRODUCT = ERP.RECID          --"ERP "PRODUCT TABLE
    --ERPIV PRODUCT INSTANCE, NEEDED FOR ATTRIBUTE RELATIONSHIP CONNECTION
    inner join ERPIV on INV_TBL.PRODUCT = ERPIV.PRODUCT
    --ERAV PRODUCT ATTRIBUTE VALUE, GETS THE ATTRIBUTE NAME
    inner join ERAV on ERPIV.RECID = ERAV.INSTANCE_VALUE
    --ERA GETS PRODUCT ATTRIBUTE RELATIONSHIP
    inner join ERA on ERAV.ATTRIBUTE = ERA.RECID
    --ERTV GETS THE ACTUAL ATTRIBUTE VALUE TEXT
    left join ERTV on ERAV.VALUE = ERTV.RECID
    --ERIV GETS the ACTUAL ATTRIBUTE VALUE FOR INT FIELDS
    left join ERIV on ERAV.VALUE = ERIV.RECID
    --ERFV GETS the ACTUAL ATTRIBUTE VALUE FOR Float FIELDS
    left join ERFV on ERAV.VALUE = ERFV.RECID
    --ERBV GETS the ACTUAL ATTRIBUTE VALUE FOR BOLLEAN FIELDS
    left join ERBV on ERAV.VALUE = ERBV.RECID
    --ERDTV GETS the ACTUAL ATTRIBUTE VALUE FOR DATE FIELDS
    /* left join ERDTV on ERAV.VALUE = ERDTV.RECID
    --ERRV GETS the ACTUAL ATTRIBUTE VALUE FOR REFERENCEN FIELDS
    left join ERRV on ERAV.VALUE = ERRV.RECID */
    group by ITEMID

),

SET_1 as (
    select distinct
        IM.ITEM_GUID as ITEM_GUID,
        -----pulling guid from item master for unique_key_generation
        IM.BUSINESS_UNIT_ADDRESS_GUID,
        IM.SOURCE_SYSTEM as SOURCE_SYSTEM,
        IM.ITEM_GUID_OLD as ITEM_GUID_OLD,
        IM.SOURCE_ITEM_IDENTIFIER,
        IM.SOURCE_BUSINESS_UNIT_CODE,
        IM.DESCRIPTION,
        IFF(SUBSTR(DESCRIPTION, 1, 7) = 'PC COMM', 1, 0) as DUMMY_PRODUCT_FLAG,
        IM.ITEM_TYPE,
        SUBSTRING(D365_IM.BRANDING, 1, REGEXP_INSTR(BRANDING, ':') - 1)
            as BRANDING_CODE,
        SUBSTRING(
            D365_IM.BRANDING, REGEXP_INSTR(BRANDING, ':') + 1, LEN(BRANDING)
        ) as BRANDING_DESC,
        0 as BRANDING_SEQ,
        SUBSTRING(
            D365_IM.PRODUCT_CLASS, 1, REGEXP_INSTR(PRODUCT_CLASS, ':') - 1
        ) as PRODUCT_CLASS_CODE,
        SUBSTRING(
            D365_IM.PRODUCT_CLASS,
            REGEXP_INSTR(PRODUCT_CLASS, ':') + 1,
            LEN(PRODUCT_CLASS)
        ) as PRODUCT_CLASS_DESC,
        0 as PRODUCT_CLASS_SEQ,
        SUBSTRING(D365_IM.SUB_PRODUCT, 1, REGEXP_INSTR(SUB_PRODUCT, ':') - 1)
            as SUB_PRODUCT_CODE,
        SUBSTRING(
            D365_IM.SUB_PRODUCT,
            REGEXP_INSTR(SUB_PRODUCT, ':') + 1,
            LEN(SUB_PRODUCT)
        ) as SUB_PRODUCT_DESC,
        0 as SUB_PRODUCT_SEQ,
        SUBSTRING(D365_IM.STRATEGIC, 1, REGEXP_INSTR(STRATEGIC, ':') - 1)
            as STRATEGIC_CODE,
        SUBSTRING(D365_IM.STRATEGIC, 1, REGEXP_INSTR(STRATEGIC, ':') - 1)
            as STRATEGIC_CODE_ALT,
        SUBSTRING(
            D365_IM.STRATEGIC, REGEXP_INSTR(STRATEGIC, ':') + 1, LEN(STRATEGIC)
        ) as STRATEGIC_DESC,
        0 as STRATEGIC_SEQ,
        SUBSTRING(D365_IM.POWERBRAND, 1, REGEXP_INSTR(POWERBRAND, ':') - 1)
            as POWER_BRAND_CODE,
        SUBSTRING(
            D365_IM.POWERBRAND,
            REGEXP_INSTR(POWERBRAND, ':') + 1,
            LEN(POWERBRAND)
        ) as POWER_BRAND_DESC,
        0 as POWER_BRAND_SEQ,
        SUBSTRING(
            D365_IM.MANUFACTURING_GROUP,
            1,
            REGEXP_INSTR(MANUFACTURING_GROUP, ':') - 1
        ) as MANUFACTURING_GROUP_CODE,
        SUBSTRING(
            D365_IM.MANUFACTURING_GROUP,
            REGEXP_INSTR(MANUFACTURING_GROUP, ':') + 1,
            LEN(MANUFACTURING_GROUP)
        ) as MANUFACTURING_GROUP_DESC,
        0 as MANUFACTURING_GROUP_SEQ,
        '-' as MANGRPCD_SITE,
        '-' as MANGRPCD_PLANT,
        '-' as MANGRPCD_COPACK_FLAG,
        SUBSTRING(D365_IM.PACK_SIZE, 1, REGEXP_INSTR(PACK_SIZE, ':') - 1)
            as PACK_SIZE_CODE,
        SUBSTRING(D365_IM.PACK_SIZE, 1, REGEXP_INSTR(PACK_SIZE, ':') - 1)
            as PACK_SIZE_CODE_ALT,
        SUBSTRING(
            D365_IM.PACK_SIZE, REGEXP_INSTR(PACK_SIZE, ':') + 1, LEN(PACK_SIZE)
        ) as PACK_SIZE_DESC,
        0 as PACK_SIZE_SEQ,
        SUBSTRING(D365_IM.CATEGORY, 1, REGEXP_INSTR(CATEGORY, ':') - 1)
            as CATEGORY_CODE,
        SUBSTRING(
            D365_IM.CATEGORY, REGEXP_INSTR(CATEGORY, ':') + 1, LEN(CATEGORY)
        ) as CATEGORY_DESC,
        0 as CATEGORY_SEQ,
        '-' as PROMO_TYPE_CODE,
        '-' as PROMO_TYPE_DESC,
        0 as PROMO_TYPE_SEQ,
        SUBSTRING(
            D365_IM.SUB_CATEGORY, 1, REGEXP_INSTR(SUB_CATEGORY, ':') - 1
        ) as SUB_CATEGORY_CODE,
        SUBSTRING(
            D365_IM.SUB_CATEGORY,
            REGEXP_INSTR(SUB_CATEGORY, ':') + 1,
            LEN(SUB_CATEGORY)
        ) as SUB_CATEGORY_DESC,
        0 as SUB_CATEGORY_SEQ,
        AX_IM.NETWEIGHT as NET_WEIGHT,
        AX_IM.NETWEIGHT as NET_WEIGHT_ALT,
        AX_IM.TARAWEIGHT as TARE_WEIGHT,
        AX_IM.TARAWEIGHT as TARE_WEIGHT_ALT,
        D365_IM.AVP_WEIGHT as AVP_WEIGHT,
        D365_IM.AVP_WEIGHT as AVP_WEIGHT_ALT,
        case
            when D365_IM.AVP_FLAG = '0' then 'N'
            when D365_IM.AVP_FLAG = '1' then 'Y' else D365_IM.AVP_FLAG
        end as AVP_FLAG,
        case
            when D365_IM.AVP_FLAG = '0' then 'N'
            when D365_IM.AVP_FLAG = '1' then 'Y' else D365_IM.AVP_FLAG
        end
            as AVP_FLAG_ALT,
        D365_IM.CONSUMER_UNITS_IN_TRADED_UNIT as CONSUMER_UNITS_IN_TRADE_UNITS,
        D365_IM.CONSUMER_UNITS_IN_TRADED_UNIT
            as CONSUMER_UNITS_IN_TRADE_UNITS_ALT,
        D365_IM.WEETABIX_PALLET_QUANTITY as PALLET_QTY,
        D365_IM.WEETABIX_PALLET_QUANTITY as PALLET_QTY_ALT,
        case
            when D365_IM.CURRENT_FLAG = '0' then 'N'
            when D365_IM.CURRENT_FLAG = '1' then 'Y' else D365_IM.CURRENT_FLAG
        end as CURRENT_FLAG,
        AX_IM.GROSSWEIGHT as GROSS_WEIGHT,
        AX_IM.GROSSDEPTH as GROSS_DEPTH,
        AX_IM.GROSSDEPTH as GROSS_DEPTH_ALT,
        AX_IM.GROSSWIDTH as GROSS_WIDTH,
        AX_IM.GROSSWIDTH as GROSS_WIDTH_ALT,
        AX_IM.GROSSHEIGHT as GROSS_HEIGHT,
        AX_IM.GROSSHEIGHT as GROSS_HEIGHT_ALT,
        D365_IM.PMP_FLAG as PMP_FLAG,
        D365_IM.PMP_FLAG as PMP_FLAG_ALT,
        CONSUMER_UNITS as CONSUMER_UNITS,
        WEETABIX_PALLET_QUANTITY_PER_LAYER as PALLET_QTY_PER_LAYER,
        WEETABIX_PALLET_QUANTITY_PER_LAYER as PALLET_QTY_PER_LAYER_ALT,
        '-' as ITEM_VAT_GROUP,
        '-' as EXCLUDE_INDICATOR,
        CC.DISPLAYVALUE as FIN_DIM_COST_CENTRE,
        FP.DISPLAYVALUE as FIN_DIM_PRODUCT,
        DP.DISPLAYVALUE as FIN_DIM_DEPARTMENT,
        SI.DISPLAYVALUE as FIN_DIM_SITE,
        WHSINVENTTABLE.FILTERCODE as WHS_FILTER_CODE,
        WHSINVENTTABLE.FILTERCODE2_ as WHS_FILTER_CODE2,
        WHSINVENTTABLE.FILTERCODE3_ as WHS_FILTER_CODE3,
        WHSINVENTTABLE.FILTERCODE4_ as WHS_FILTER_CODE4,
        PDSSHELFLIFE as SHELF_LIFE_DAYS,
        D365_IM.PALLET_TYPE as PALLET_TYPE,
        D365_IM.PALLET_CONFIGURATION as PALLET_CONFIG,
        IM.load_date as DATE_INSERTED,
        IM.update_date as DATE_UPDATED
    from IM
    inner join
        D365_IM
        on UPPER(TRIM(IM.SOURCE_ITEM_IDENTIFIER)) = UPPER(TRIM(D365_IM.ITEM_ID))
    left join AX_IM
        on AX_IM.ITEMID = IM.SOURCE_ITEM_IDENTIFIER
    ------Get the Cost Centre
    left join
        CC
        on
            AX_IM.PARTITION = CC.PARTITION
            and AX_IM.DEFAULTDIMENSION = CC.DIMENSIONATTRIBUTEVALUESET
    left join
        FP
        on
            AX_IM.PARTITION = FP.PARTITION
            and AX_IM.DEFAULTDIMENSION = FP.DIMENSIONATTRIBUTEVALUESET
    left join
        DP
        on
            AX_IM.PARTITION = DP.PARTITION
            and AX_IM.DEFAULTDIMENSION = DP.DIMENSIONATTRIBUTEVALUESET
    left join
        SI
        on
            AX_IM.PARTITION = SI.PARTITION
            and AX_IM.DEFAULTDIMENSION = SI.DIMENSIONATTRIBUTEVALUESET
    left join
        /* From inner join making this as left join to
         allow the newly created item to flow through */
        WHSINVENTTABLE
        on WHSINVENTTABLE.ITEMID = IM.SOURCE_ITEM_IDENTIFIER
),


/*removed 2nd union  which is no longer required */



FINAL as (
    select
        *,
        ROW_NUMBER()
            over (
                partition by
                    SOURCE_SYSTEM,
                    SOURCE_ITEM_IDENTIFIER,
                    SOURCE_BUSINESS_UNIT_CODE
                order by 1
            )
            as ROWNUM
    from SET_1
)



select
    ITEM_GUID,
    BUSINESS_UNIT_ADDRESS_GUID,
    SOURCE_SYSTEM,
    ITEM_GUID_OLD,
    SOURCE_ITEM_IDENTIFIER,
    SOURCE_BUSINESS_UNIT_CODE,
    DESCRIPTION,
    DUMMY_PRODUCT_FLAG,
    ITEM_TYPE,
    (TRIM(BRANDING_CODE)) as BRANDING_CODE,
    (TRIM(BRANDING_DESC)) as BRANDING_DESC,
    BRANDING_SEQ,
    (TRIM(PRODUCT_CLASS_CODE)) as PRODUCT_CLASS_CODE,
    (TRIM(PRODUCT_CLASS_DESC)) as PRODUCT_CLASS_DESC,
    PRODUCT_CLASS_SEQ,
    (TRIM(SUB_PRODUCT_CODE)) as SUB_PRODUCT_CODE,
    (TRIM(SUB_PRODUCT_DESC)) as SUB_PRODUCT_DESC,
    SUB_PRODUCT_SEQ,
    (TRIM(STRATEGIC_CODE)) as STRATEGIC_CODE,
    (TRIM(STRATEGIC_CODE_ALT)) as STRATEGIC_CODE_ALT,
    (TRIM(STRATEGIC_DESC)) as STRATEGIC_DESC,
    STRATEGIC_SEQ,
    (TRIM(POWER_BRAND_CODE)) as POWER_BRAND_CODE,
    (TRIM(POWER_BRAND_DESC)) as POWER_BRAND_DESC,
    POWER_BRAND_SEQ,
    (TRIM(MANUFACTURING_GROUP_CODE)) as MANUFACTURING_GROUP_CODE,
    (TRIM(MANUFACTURING_GROUP_DESC)) as MANUFACTURING_GROUP_DESC,
    MANUFACTURING_GROUP_SEQ,
    MANGRPCD_SITE,
    MANGRPCD_PLANT,
    MANGRPCD_COPACK_FLAG,
    (TRIM(PACK_SIZE_CODE)) as PACK_SIZE_CODE,
    (TRIM(PACK_SIZE_CODE_ALT)) as PACK_SIZE_CODE_ALT,
    (TRIM(PACK_SIZE_DESC)) as PACK_SIZE_DESC,
    PACK_SIZE_SEQ,
    (TRIM(CATEGORY_CODE)) as CATEGORY_CODE,
    (TRIM(CATEGORY_DESC)) as CATEGORY_DESC,
    CATEGORY_SEQ,
    PROMO_TYPE_CODE,
    PROMO_TYPE_DESC,
    PROMO_TYPE_SEQ,
    (TRIM(SUB_CATEGORY_CODE)) as SUB_CATEGORY_CODE,
    (TRIM(SUB_CATEGORY_DESC)) as SUB_CATEGORY_DESC,
    SUB_CATEGORY_SEQ,
    NET_WEIGHT,
    NET_WEIGHT_ALT,
    TARE_WEIGHT,
    TARE_WEIGHT_ALT,
    AVP_WEIGHT,
    AVP_WEIGHT_ALT,
    AVP_FLAG as AVP_FLAG,
    AVP_FLAG_ALT,
    CONSUMER_UNITS_IN_TRADE_UNITS,
    CONSUMER_UNITS_IN_TRADE_UNITS_ALT,
    PALLET_QTY,
    PALLET_QTY_ALT,
    CURRENT_FLAG,
    GROSS_WEIGHT,
    GROSS_DEPTH,
    GROSS_DEPTH_ALT,
    GROSS_WIDTH,
    GROSS_WIDTH_ALT,
    GROSS_HEIGHT,
    GROSS_HEIGHT_ALT,
    PMP_FLAG,
    PMP_FLAG_ALT,
    CONSUMER_UNITS,
    PALLET_QTY_PER_LAYER,
    PALLET_QTY_PER_LAYER_ALT,
    ITEM_VAT_GROUP,
    EXCLUDE_INDICATOR,
    FIN_DIM_COST_CENTRE,
    FIN_DIM_PRODUCT,
    FIN_DIM_DEPARTMENT,
    FIN_DIM_SITE,
    WHS_FILTER_CODE,
    WHS_FILTER_CODE2,
    WHS_FILTER_CODE3,
    WHS_FILTER_CODE4,
    SHELF_LIFE_DAYS,
    NVL(PALLET_TYPE, '-') as PALLET_TYPE,
    NVL(PALLET_CONFIG, '-') as PALLET_CONFIG,
    DATE_INSERTED,
    DATE_UPDATED
from FINAL where ROWNUM = 1
