with dim_coa as 

(
    select * from {{ ref('src_dbix_tbldimchartofaccounts') }}
),

ref_lt as
(
    select * from {{ ref('src_dbix_tblrefaxledgertable') }}
),

test_coa_hierarchy as 
(
    select distinct
a.kcoa_line,
a.dca_coa_linesequence,
 a.dca_coa_lineparent,
a.dca_accountdescription,
b.dca_accountdescription as dca_parentdescription,
a.dca_unaryoperator as unaryoperator
from dim_coa a
left outer join
dim_coa b
on 
case when a.dca_coa_lineparent = a.kcoa_line
then null
else a.dca_coa_lineparent end = b.kcoa_line
),

    weetabix_coa_recursive_temp as (

        select 0 as depth, '' as parent_path, '' as parent_path_name, *
        from test_coa_hierarchy
        where dca_parentdescription is null

        union all

        select
            depth + 1 as depth,
            case
                when b.parent_path = ''
                then to_char(a.dca_coa_lineparent)
                else b.parent_path || '|' || to_char(a.dca_coa_lineparent)
            end as parent_path,
            case
                when b.parent_path_name = ''
                then to_char(a.dca_parentdescription)
                else b.parent_path_name || '|' || to_char(a.dca_parentdescription)
            end as parent_path_name,
            a.*
        from test_coa_hierarchy a
        join weetabix_coa_recursive_temp b on a.dca_coa_lineparent = b.kcoa_line
        where a.dca_parentdescription is not null
    ),



    x as (
        select accountnum, accountname, costcentre, kcoa_line
        from ref_lt
        where kcoa_line is not null
    ),


    src as (
        select distinct
            x.accountnum,
            x.costcentre,
            depth + 1 as depth,
            depth + 1 + 2 as level_no,
            a.parent_path
            || '|'
            || to_char(a.kcoa_line)
            || '|'
            || x.costcentre
            || '|'
            || x.accountnum as parent_path,
            a.parent_path_name
            || '|'
            || to_char(a.dca_accountdescription)
            || '|'
            || x.costcentre
            || '|'
            || x.accountnum as parent_path_name,
            a.kcoa_line,
            a.dca_coa_linesequence,
            a.dca_coa_lineparent,
            a.dca_accountdescription,
            a.dca_parentdescription,
            a.unaryoperator
        from weetabix_coa_recursive_temp a
        left outer join x on a.kcoa_line = x.kcoa_line
        where (x.accountnum is not null and x.costcentre is not null)
    ),

    final as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            'ACCOUNT' as hier_category,
            'ACCOUNT-COA' as hier_name,
            src.costcentre || '|' || src.accountnum as account_code,
            src.costcentre || '|' || src.accountnum as leaf_node,
            src.depth as node_level,
            case
                when src.level_no >= 1
                then regexp_substr(src.parent_path, '[^|]+', 1, 1)
                else ' '
            end as level1_id,
            case
                when src.level_no >= 1
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 1)
                else ' '
            end as level1_name,
            case
                when src.level_no >= 2
                then regexp_substr(src.parent_path, '[^|]+', 1, 2)
                else ' '
            end as level2_id,
            case
                when src.level_no >= 2
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 2)
                else ' '
            end as level2_name,
            case
                when src.level_no >= 3
                then regexp_substr(src.parent_path, '[^|]+', 1, 3)
                else ' '
            end as level3_id,
            case
                when src.level_no >= 3
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 3)
                else ' '
            end as level3_name,
            case
                when src.level_no >= 4
                then regexp_substr(src.parent_path, '[^|]+', 1, 4)
                else ' '
            end as level4_id,
            case
                when src.level_no >= 4
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 4)
                else ' '
            end as level4_name,
            case
                when src.level_no >= 5
                then regexp_substr(src.parent_path, '[^|]+', 1, 5)
                else ' '
            end as level5_id,
            case
                when src.level_no >= 5
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 5)
                else ' '
            end as level5_name,
            case
                when src.level_no >= 6
                then regexp_substr(src.parent_path, '[^|]+', 1, 6)
                else ' '
            end as level6_id,
            case
                when src.level_no >= 6
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 6)
                else ' '
            end as level6_name,
            case
                when src.level_no >= 7
                then regexp_substr(src.parent_path, '[^|]+', 1, 7)
                else ' '
            end as level7_id,
            case
                when src.level_no >= 7
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 7)
                else ' '
            end as level7_name,
            case
                when src.level_no >= 8
                then regexp_substr(src.parent_path, '[^|]+', 1, 8)
                else ' '
            end as level8_id,
            case
                when src.level_no >= 8
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 8)
                else ' '
            end as level8_name,
            case
                when src.level_no >= 9
                then regexp_substr(src.parent_path, '[^|]+', 1, 9)
                else ' '
            end as level9_id,
            case
                when src.level_no >= 9
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 9)
                else ' '
            end as level9_name,
            case
                when src.level_no >= 10
                then regexp_substr(src.parent_path, '[^|]+', 1, 10)
                else ' '
            end as level10_id,
            case
                when src.level_no >= 10
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 10)
                else ' '
            end as level10_name,
            case
                when src.level_no >= 11
                then regexp_substr(src.parent_path, '[^|]+', 1, 11)
                else ' '
            end as level11_id,
            case
                when src.level_no >= 11
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 11)
                else ' '
            end as level11_name,
            case
                when src.level_no >= 12
                then regexp_substr(src.parent_path, '[^|]+', 1, 12)
                else ' '
            end as level12_id,
            case
                when src.level_no >= 12
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 12)
                else ' '
            end as level12_name,
            case
                when src.level_no >= 13
                then regexp_substr(src.parent_path, '[^|]+', 1, 13)
                else ' '
            end as level13_id,
            case
                when src.level_no >= 13
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 13)
                else ' '
            end as level13_name,
            case
                when src.level_no >= 14
                then regexp_substr(src.parent_path, '[^|]+', 1, 14)
                else ' '
            end as level14_id,
            case
                when src.level_no >= 14
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 14)
                else ' '
            end as level14_name,
            case
                when src.level_no >= 15
                then regexp_substr(src.parent_path, '[^|]+', 1, 15)
                else ' '
            end as level15_id,
            case
                when src.level_no >= 15
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 15)
                else ' '
            end as level15_name,
            case
                when src.level_no >= 16
                then regexp_substr(src.parent_path, '[^|]+', 1, 16)
                else ' '
            end as level16_id,
            case
                when src.level_no >= 16
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 16)
                else ' '
            end as level16_name,
            case
                when src.level_no >= 17
                then regexp_substr(src.parent_path, '[^|]+', 1, 17)
                else ' '
            end as level17_id,
            case
                when src.level_no >= 17
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 17)
                else ' '
            end as level17_name,
            case
                when src.level_no >= 18
                then regexp_substr(src.parent_path, '[^|]+', 1, 18)
                else ' '
            end as level18_id,
            case
                when src.level_no >= 18
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 18)
                else ' '
            end as level18_name,
            case
                when src.level_no >= 19
                then regexp_substr(src.parent_path, '[^|]+', 1, 19)
                else ' '
            end as level19_id,
            case
                when src.level_no >= 19
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 19)
                else ' '
            end as level19_name,
            case
                when src.level_no >= 20
                then regexp_substr(src.parent_path, '[^|]+', 1, 20)
                else ' '
            end as level20_id,
            case
                when src.level_no >= 20
                then regexp_substr(src.parent_path_name, '[^|]+', 1, 20)
                else ' '
            end as level20_name,
            CURRENT_DATE                              AS LOAD_DATE,
    CURRENT_DATE                              AS UPDATE_DATE
        from src
    )

SELECT CAST (TRIM (SOURCE_SYSTEM) AS VARCHAR2 (60)) AS SOURCE_SYSTEM,
    CAST (TRIM (HIER_CATEGORY) AS VARCHAR2 (60))          AS HIER_CATEGORY,
    CAST (TRIM (HIER_NAME) AS VARCHAR2 (60))      AS HIER_NAME,
    CAST (TRIM (nvl(ACCOUNT_CODE,'')) AS VARCHAR2 (60))  AS tagetik_account,
    CAST (TRIM (nvl(LEAF_NODE,'')) AS VARCHAR2 (60))  AS LEAF_NODE,
    CAST (TRIM (NODE_LEVEL) AS NUMBER (15))    AS NODE_LEVEL,
    CAST (TRIM (LEVEL1_ID) AS VARCHAR2 (60))     AS NODE_1,
    CAST (TRIM (LEVEL1_NAME) AS VARCHAR2 (255))  AS DESC_1,
    CAST (TRIM (LEVEL2_ID) AS VARCHAR2 (60))     AS NODE_2,
    CAST (TRIM (LEVEL2_NAME) AS VARCHAR2 (255))  AS DESC_2,
    CAST (TRIM (LEVEL3_ID) AS VARCHAR2 (60))     AS NODE_3,
    CAST (TRIM (LEVEL3_NAME) AS VARCHAR2 (255))  AS DESC_3,
    CAST (TRIM (LEVEL4_ID) AS VARCHAR2 (60))     AS NODE_4,
    CAST (TRIM (LEVEL4_NAME) AS VARCHAR2 (255))  AS DESC_4,
    CAST (TRIM (LEVEL5_ID) AS VARCHAR2 (60))     AS NODE_5,
    CAST (TRIM (LEVEL5_NAME) AS VARCHAR2 (255))  AS DESC_5,
    CAST (TRIM (LEVEL6_ID) AS VARCHAR2 (60))     AS NODE_6,
    CAST (TRIM (LEVEL6_NAME) AS VARCHAR2 (255))  AS DESC_6,
    CAST (TRIM (LEVEL7_ID) AS VARCHAR2 (60))     AS NODE_7,
    CAST (TRIM (LEVEL7_NAME) AS VARCHAR2 (255))  AS DESC_7,
    CAST (TRIM (LEVEL8_ID) AS VARCHAR2 (60))     AS NODE_8,
    CAST (TRIM (LEVEL8_NAME) AS VARCHAR2 (255))  AS DESC_8,
    CAST (TRIM (LEVEL9_ID) AS VARCHAR2 (60))     AS NODE_9,
    CAST (TRIM (LEVEL9_NAME) AS VARCHAR2 (255))  AS DESC_9,
    CAST (TRIM (LEVEL10_ID) AS VARCHAR2 (60))    AS NODE_10,
    CAST (TRIM (LEVEL10_NAME) AS VARCHAR2 (255)) AS DESC_10,
    CAST (TRIM (LEVEL11_ID) AS VARCHAR2 (60))    AS NODE_11,
    CAST (TRIM (LEVEL11_NAME) AS VARCHAR2 (255)) AS DESC_11,
    CAST (TRIM (LEVEL12_ID) AS VARCHAR2 (60))    AS NODE_12,
    CAST (TRIM (LEVEL12_NAME) AS VARCHAR2 (255)) AS DESC_12,
    CAST (TRIM (LEVEL13_ID) AS VARCHAR2 (60))    AS NODE_13,
    CAST (TRIM (LEVEL13_NAME) AS VARCHAR2 (255)) AS DESC_13,
    CAST (TRIM (LEVEL14_ID) AS VARCHAR2 (60))    AS NODE_14,
    CAST (TRIM (LEVEL14_NAME) AS VARCHAR2 (255)) AS DESC_14,
    CAST (TRIM (LEVEL15_ID) AS VARCHAR2 (60))    AS NODE_15,
    CAST (TRIM (LEVEL15_NAME) AS VARCHAR2 (255)) AS DESC_15,
    CAST (TRIM (LEVEL16_ID) AS VARCHAR2 (60))    AS NODE_16,
    CAST (TRIM (LEVEL16_NAME) AS VARCHAR2 (255)) AS DESC_16,
    CAST (TRIM (LEVEL17_ID) AS VARCHAR2 (60))    AS NODE_17,
    CAST (TRIM (LEVEL17_NAME) AS VARCHAR2 (255)) AS DESC_17,
    CAST (TRIM (LEVEL18_ID) AS VARCHAR2 (60))    AS NODE_18,
    CAST (TRIM (LEVEL18_NAME) AS VARCHAR2 (255)) AS DESC_18,
    CAST (TRIM (LEVEL19_ID) AS VARCHAR2 (60))    AS NODE_19,
    CAST (TRIM (LEVEL19_NAME) AS VARCHAR2 (255)) AS DESC_19,
    CAST (TRIM (LEVEL20_ID) AS VARCHAR2 (60))    AS NODE_20,
    CAST (TRIM (LEVEL20_NAME) AS VARCHAR2 (255)) AS DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from Final
