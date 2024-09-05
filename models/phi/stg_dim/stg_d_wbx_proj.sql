with PT as (
    select * from {{ ref('src_projtable') }}
),


DAV as (
    select * from {{ ref('src_dimensionattributevalue') }}
),

DAVS as (
    select * from {{ ref('src_dimensionattributevaluesetitem') }}
),

SORT as (
    select * from {{ ref('src_projsorting') }}
),

DA as (
    select * from {{ ref('src_dimensionattribute') }}
),

HCM as (
    select * from {{ ref('src_hcmworker') }}
),

DIRPARTYTABLE as (
    select * from {{ ref('src_dirpartytable') }}
),

SRS as (
    select * from {{ ref('src_srsanalysisenums') }}
),

D1 as (
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

D2 as (
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


D3 as (
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

D4 as (
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
    where DA.NAME in ('Plant')
),

D5 as (
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
    where DA.NAME in ('Customer')
),

D6 as (
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

D7 as (
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
    where DA.NAME in ('Purpose')
),

PROJTYPE as (
    select
        ENUMITEMVALUE,
        ENUMITEMLABEL,
        ENUMITEMNAME
    from SRS
    where ENUMNAME = 'ProjType' --and languageid = 'en-gb' /*Commenting out this filter as we don't have data for languageid in d365 src table. Avinash-052024*/
),

PROJSTATUS as (
    select
        ENUMITEMVALUE,
        ENUMITEMLABEL,
        ENUMITEMNAME
    from SRS
    where ENUMNAME = 'ProjStatus' --and languageid = 'en-gb' /*Commenting out this filter as we don't have data for languageid in d365 src table. Avinash-052024*/
),

SORT0 as (
    select
        DATAAREAID,
        SORTINGID,
        DESCRIPTION
    from SORT where SORTCODE = 0
),

SORT1 as (
    select
        DATAAREAID,
        SORTINGID,
        DESCRIPTION
    from SORT where SORTCODE = 1
),


SORT2 as (
    select
        DATAAREAID,
        SORTINGID,
        DESCRIPTION
    from SORT where SORTCODE = 2
),

STG as (
    select
        '{{ env_var("DBT_SOURCE_SYSTEM") }}' as SOURCE_SYSTEM,
        PT.PROJID as PROJECT_ID,
        PT.NAME as DESCRIPTION,
        PT.STATUS as PROJECT_STATUS,
        NVL(PROJSTATUS.ENUMITEMLABEL, 'N/A') as PROJECT_STATUS_DESCR,
        PT.PROJGROUPID as PROJECT_GROUP,
        PT.TYPE as PROJECT_TYPE,
        NVL(PROJTYPE.ENUMITEMLABEL, 'N/A') as PROJECT_TYPE_DESCR,
        IFNULL(D1.DISPLAYVALUE, '') as SITE,
        IFNULL(D2.DISPLAYVALUE, '') as DEPARTMENT,
        IFNULL(D3.DISPLAYVALUE, '') as COST_CENTER,
        IFNULL(D4.DISPLAYVALUE, '') as PLANT,
        IFNULL(D5.DISPLAYVALUE, '') as CUSTOMER,
        IFNULL(D6.DISPLAYVALUE, '') as PRODUCT,
        IFNULL(D7.DISPLAYVALUE, '') as CAF_NO,
        PT.SORTINGID as SORTINGID,
        SORT0.DESCRIPTION as SORTINGID_DESCR,
        PT.SORTINGID2_ as SORTINGID2_,
        SORT1.DESCRIPTION as SORTINGID2_DESCR,
        PT.SORTINGID3_ as SORTINGID3_,
        SORT2.DESCRIPTION as SORTINGID3_DESCR,
        PT.CREATED as CREATION_DATE,
        PT.PROJECTEDSTARTDATE as START_DATE_PROJECTED,
        PT.STARTDATE as START_DATE_ACTUAL,
        PT.PROJECTEDENDDATE as END_DATE_PROJECTED,
        PT.ENDDATE as END_DATE_ACTUAL,
        PT.EXTENSIONDATE as EXTENSION_DATE,
        PT.MODIFIEDDATETIME as SOURCE_UPDATE_DATE,
        PT.WORKERRESPONSIBLEFINANCIAL as PROJECT_CONTROLLER_ID,
        DF.NAME as PROJECT_CONTROLLER,
        PT.WORKERRESPONSIBLE as PROJECT_MANAGER_ID,
        DM.NAME as PROJECT_MANAGER,
        PT.WORKERRESPONSIBLESALES as SALES_MANAGER_ID,
        DS.NAME as SALES_MANAGER
    from
        PT
    left join
        D1
        on
            PT.PARTITION = D1.PARTITION
            and PT.DEFAULTDIMENSION = D1.DIMENSIONATTRIBUTEVALUESET
    left join
        D2
        on
            PT.PARTITION = D2.PARTITION
            and PT.DEFAULTDIMENSION = D2.DIMENSIONATTRIBUTEVALUESET
    left join
        D3
        on
            PT.PARTITION = D3.PARTITION
            and PT.DEFAULTDIMENSION = D3.DIMENSIONATTRIBUTEVALUESET
    left join
        D4
        on
            PT.PARTITION = D4.PARTITION
            and PT.DEFAULTDIMENSION = D4.DIMENSIONATTRIBUTEVALUESET
    left join
        D5
        on
            PT.PARTITION = D5.PARTITION
            and PT.DEFAULTDIMENSION = D5.DIMENSIONATTRIBUTEVALUESET
    left join
        D6
        on
            PT.PARTITION = D6.PARTITION
            and PT.DEFAULTDIMENSION = D6.DIMENSIONATTRIBUTEVALUESET
    left join
        D7
        on
            PT.PARTITION = D7.PARTITION
            and PT.DEFAULTDIMENSION = D7.DIMENSIONATTRIBUTEVALUESET
    left join PROJTYPE on PT.TYPE = PROJTYPE.ENUMITEMVALUE
    left join PROJSTATUS on PT.STATUS = PROJSTATUS.ENUMITEMVALUE
    left join
        SORT0
        on
            TRIM(UPPER(PT.SORTINGID)) = TRIM(UPPER(SORT0.SORTINGID))
            and TRIM(UPPER(PT.DATAAREAID)) = TRIM(UPPER(SORT0.DATAAREAID))
    left join
        SORT1
        on
            TRIM(UPPER(PT.SORTINGID2_)) = TRIM(UPPER(SORT1.SORTINGID))
            and TRIM(UPPER(PT.DATAAREAID)) = TRIM(UPPER(SORT1.DATAAREAID))
    left join
        SORT2
        on
            TRIM(UPPER(PT.SORTINGID3_)) = TRIM(UPPER(SORT2.SORTINGID))
            and TRIM(UPPER(PT.DATAAREAID)) = TRIM(UPPER(SORT2.DATAAREAID))
    --Financial
    left outer join
        HCM as HF
        on
            PT.PARTITION = HF.PARTITION
            and PT.WORKERRESPONSIBLEFINANCIAL = HF.RECID
    left outer join
        DIRPARTYTABLE as DF
        on
            DF.INSTANCERELATIONTYPE = 13438
            and HF.PARTITION = DF.PARTITION
            and HF.PERSON = DF.RECID
    --Manager
    left outer join
        HCM as HM
        on PT.PARTITION = HM.PARTITION and PT.WORKERRESPONSIBLE = HM.RECID
    left outer join
        DIRPARTYTABLE as DM
        on
            DM.INSTANCERELATIONTYPE = 13438
            and HM.PARTITION = DM.PARTITION
            and HM.PERSON = DM.RECID
    --Sales
    left outer join
        HCM as HS
        on PT.PARTITION = HS.PARTITION and PT.WORKERRESPONSIBLESALES = HS.RECID
    left outer join
        DIRPARTYTABLE as DS
        on
            DS.INSTANCERELATIONTYPE = 13438
            and HS.PARTITION = DS.PARTITION
            and HS.PERSON = DS.RECID
),

FINAL as (
    select
        SOURCE_SYSTEM,
        PROJECT_ID,
        DESCRIPTION,
        TO_CHAR(TO_NUMBER(PROJECT_STATUS)) as PROJECT_STATUS,
        PROJECT_STATUS_DESCR,
        PROJECT_GROUP,
        TO_CHAR(TO_NUMBER(PROJECT_TYPE)) as PROJECT_TYPE,
        PROJECT_TYPE_DESCR,
        SITE,
        DEPARTMENT,
        COST_CENTER,
        PLANT,
        CUSTOMER,
        PRODUCT,
        CAF_NO,
        SORTINGID,
        SORTINGID_DESCR,
        SORTINGID2_,
        SORTINGID2_DESCR,
        SORTINGID3_,
        SORTINGID3_DESCR,
        CREATION_DATE,
        START_DATE_PROJECTED,
        START_DATE_ACTUAL,
        END_DATE_PROJECTED,
        END_DATE_ACTUAL,
        EXTENSION_DATE,
        SOURCE_UPDATE_DATE,
        PROJECT_CONTROLLER_ID,
        PROJECT_CONTROLLER,
        PROJECT_MANAGER_ID,
        PROJECT_MANAGER,
        SALES_MANAGER_ID,
        SALES_MANAGER,
        SYSDATE() as LOAD_DATE,
        SYSDATE() as UPDATE_DATE

    from STG
)


select
    {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','PROJECT_ID']) }}
        as PROJECT_GUID,
    *
from FINAL