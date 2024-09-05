with PT as (
    select * from {{ ref('src_projtable') }}
),

PBl as (
    select * from {{ ref('src_projbudgetline') }}
),

SRS as (
    select * from {{ ref('src_srsanalysisenums') }}
),

TRANSTYPE as (
    select
        ENUMITEMVALUE,
        ENUMITEMLABEL,
        ENUMITEMNAME
    from SRS where ENUMNAME = 'ProjType' --and languageid = 'en-gb'  /*Commenting out this filter as we don't have data for languageid in d365 src table. Avinash-052024*/
),

Final as (
    select
        '{{ env_var("DBT_SOURCE_SYSTEM") }}' as SOURCE_SYSTEM,
        PT.PROJID as PROJECT_ID,
        PBL.PROJTRANSTYPE as PROJECT_TRANS_TYPE,
        TRANSTYPE.ENUMITEMLABEL as PROJECT_TRANS_DESCR,
        PBL.CATEGORYID as PROJECT_BUDG_CATEGORY,
        PBL.ORIGINALBUDGET as ORIGINAL_BUDGET,
        PBL.COMMITTEDREVISIONS as COMMITTED_REVISIONS,
        PBL.UNCOMMITTEDREVISIONS as UNCOMMITTED_REVISIONS,
        PBL.TOTALBUDGET as TOTAL_BUDGET
    from PT
    inner join PBL on PT.DATAAREAID = PBL.DATAAREAID and PT.PROJID = PBL.PROJID
    left join TRANSTYPE on PBL.PROJTRANSTYPE = TRANSTYPE.ENUMITEMVALUE
)

select
    {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','PROJECT_ID']) }}
        as PROJECT_GUID,
    *
from Final