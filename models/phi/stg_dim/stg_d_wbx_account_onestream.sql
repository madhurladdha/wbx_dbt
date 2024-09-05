with onestream_hierarchy as (
    select * from {{ ref('src_ref_onestream_hier_stg') }}
),

onestream_hierarchy_lvl8 as (
    select * from {{ ref('src_ref_onestream_hier_stg') }} where XFK_METADATAROOT_DIMENSIONS_DI = '8'
),

onestream_hierarchy_lvl6 as (
    select * from {{ ref('src_ref_onestream_hier_stg') }} where XFK_METADATAROOT_DIMENSIONS_DI = '6'
),

onestream_descriptions as (
    select * from {{ ref('src_ref_onestream_desc_xref') }} where description NOT IN ('Non-Controlling Interest','Net Income/Loss','Various Countries') and name <> 'Top'
),

acct_hier_8 AS
( 
    SELECT onestream_hierarchy.aggregationweight,
        onestream_hierarchy.child,
        onestream_hierarchy.parent,
        onestream_hierarchy.xfk_metadataroot_dimensions_di,
        onestream_hierarchy.xpk_metadataroot_dimensions_di,
        level as L,
        SYS_CONNECT_BY_PATH (onestream_hierarchy.CHILD, '|') PTH_NODE,
        SYS_CONNECT_BY_PATH (onestream_descriptions.description, '|') PTH_DESC
    FROM onestream_hierarchy
    LEFT JOIN onestream_descriptions
        ON onestream_hierarchy.child = onestream_descriptions.name
        WHERE onestream_hierarchy.XFK_METADATAROOT_DIMENSIONS_DI = 8
        START WITH onestream_hierarchy.PARENT = 'root' AND child <> 'Top'
        CONNECT BY PRIOR onestream_hierarchy.child = onestream_hierarchy.PARENT
),

acct_hier_6 AS
( 
    SELECT onestream_hierarchy.aggregationweight,
        onestream_hierarchy.child,
        onestream_hierarchy.parent,
        onestream_hierarchy.xfk_metadataroot_dimensions_di,
        onestream_hierarchy.xpk_metadataroot_dimensions_di,
        level as L,
        SYS_CONNECT_BY_PATH (onestream_hierarchy.CHILD, '|') PTH_NODE,
        SYS_CONNECT_BY_PATH (onestream_descriptions.description, '|') PTH_DESC
    FROM onestream_hierarchy
    LEFT JOIN onestream_descriptions
        ON onestream_hierarchy.child = onestream_descriptions.name
        WHERE onestream_hierarchy.XFK_METADATAROOT_DIMENSIONS_DI = 6
        START WITH onestream_hierarchy.PARENT = 'root' AND child <> 'Top'
        CONNECT BY PRIOR onestream_hierarchy.child = onestream_hierarchy.PARENT
),

acct_hier_ebitda AS
(
    select 
        'EBITDA_HIER' as source_system,
        'ACCOUNT' as hier_category,
        'EBITDA_ACCOUNT' as hier_name,
        acct_hier_8.child as account_code,
        acct_hier_8.child as leaf_node,
        acct_hier_8.l as node_level,
        acct_hier_8.pth_node as pth_node,
        acct_hier_8.pth_desc as pth_desc
    from acct_hier_8
    left join onestream_hierarchy_lvl8
    ON acct_hier_8.child = onestream_hierarchy_lvl8.parent
    WHERE onestream_hierarchy_lvl8.parent IS NULL 
    and acct_hier_8.child <> 'Blank'
    and SPLIT_PART(PTH_NODE,'|',3) = 'NIavailCS'
),

acct_hier_onestream_legacy AS
(
    select 
        'TAGETIK_ACCOUNT' as source_system,
        'ACCOUNT' as hier_category,
        'TAGETIK_ACCOUNT' as hier_name,
        acct_hier_8.child as account_code,
        acct_hier_8.child as leaf_node,
        acct_hier_8.l as node_level,
        acct_hier_8.pth_node as pth_node,
        acct_hier_8.pth_desc as pth_desc
    from acct_hier_8
    left join onestream_hierarchy_lvl8
    ON acct_hier_8.child = onestream_hierarchy_lvl8.parent
    WHERE onestream_hierarchy_lvl8.parent IS NULL 
    and acct_hier_8.child <> 'Blank'
    and SPLIT_PART(PTH_NODE,'|',3) <> 'NIavailCS'
),

acct_hier_onestream_organization AS
(
    select 
        'ONESTREAM_ENTITY' as source_system,
        'COMPANY' as hier_category,
        'ENTITY' as hier_name,
        acct_hier_6.child as account_code,
        acct_hier_6.child as leaf_node,
        acct_hier_6.l as node_level,
        acct_hier_6.pth_node as pth_node,
        acct_hier_6.pth_desc as pth_desc
    from acct_hier_6
    left join onestream_hierarchy_lvl6
    ON acct_hier_6.child = onestream_hierarchy_lvl6.parent
    WHERE onestream_hierarchy_lvl6.parent IS NULL 
    and acct_hier_6.child <> 'Blank'
    and SPLIT_PART(PTH_NODE,'|',2) = 'Segment'
),

results_union AS
(
    select * from acct_hier_ebitda
    union
    select * from acct_hier_onestream_legacy
    union
    select * from acct_hier_onestream_organization
),

final as(SELECT 
  source_system,
  hier_category,
  hier_name,
  account_code,
  leaf_node,
  node_level,
  SPLIT_PART(PTH_NODE,'|',2) NODE_1,
  SPLIT_PART(PTH_DESC,'|',2) DESC_1,
  SPLIT_PART(PTH_NODE,'|',3) NODE_2,
  SPLIT_PART(PTH_DESC,'|',3) DESC_2,
  SPLIT_PART(PTH_NODE,'|',4) NODE_3,
  SPLIT_PART(PTH_DESC,'|',4) DESC_3,
  SPLIT_PART(PTH_NODE,'|',5) NODE_4,
  SPLIT_PART(PTH_DESC,'|',5) DESC_4,
  SPLIT_PART(PTH_NODE,'|',6) NODE_5,
  SPLIT_PART(PTH_DESC,'|',6) DESC_5,
  SPLIT_PART(PTH_NODE,'|',7) NODE_6,
  SPLIT_PART(PTH_DESC,'|',7) DESC_6,
  SPLIT_PART(PTH_NODE,'|',8) NODE_7,
  SPLIT_PART(PTH_DESC,'|',8) DESC_7,
  SPLIT_PART(PTH_NODE,'|',9) NODE_8,
  SPLIT_PART(PTH_DESC,'|',9) DESC_8,
  SPLIT_PART(PTH_NODE,'|',10) NODE_9,
  SPLIT_PART(PTH_DESC,'|',10) DESC_9,
  SPLIT_PART(PTH_NODE,'|',11) NODE_10,
  SPLIT_PART(PTH_DESC,'|',11) DESC_10,
  SPLIT_PART(PTH_NODE,'|',12) NODE_11,
  SPLIT_PART(PTH_DESC,'|',12) DESC_11,
  SPLIT_PART(PTH_NODE,'|',13) NODE_12,
  SPLIT_PART(PTH_DESC,'|',13) DESC_12,
  SPLIT_PART(PTH_NODE,'|',14) NODE_13,
  SPLIT_PART(PTH_DESC,'|',14) DESC_13,
  SPLIT_PART(PTH_NODE,'|',15) NODE_14,
  SPLIT_PART(PTH_DESC,'|',15) DESC_14,
  SPLIT_PART(PTH_NODE,'|',16) NODE_15,
  SPLIT_PART(PTH_DESC,'|',16) DESC_15,
  SPLIT_PART(PTH_NODE,'|',17) NODE_16,
  SPLIT_PART(PTH_DESC,'|',17) DESC_16,
  SPLIT_PART(PTH_NODE,'|',18) NODE_17,
  SPLIT_PART(PTH_DESC,'|',18) DESC_17,
  SPLIT_PART(PTH_NODE,'|',19) NODE_18,
  SPLIT_PART(PTH_DESC,'|',19) DESC_18,
  SPLIT_PART(PTH_NODE,'|',20) NODE_19,
  SPLIT_PART(PTH_DESC,'|',20) DESC_19,
  SPLIT_PART(PTH_NODE,'|',21) NODE_20,
  SPLIT_PART(PTH_DESC,'|',21) DESC_20,
  CURRENT_DATE AS LOAD_DATE,
  CURRENT_DATE AS UPDATE_DATE
  from results_union
)

 select 
        cast (trim (source_system) as varchar2 (60)) as source_system,
        cast (trim (hier_category) as varchar2 (60)) as hier_category,
        cast (trim (hier_name) as varchar2 (60))     as hier_name,
        cast (trim (account_code) as varchar2 (60))  as tagetik_account,
        cast (trim (leaf_node) as varchar2 (60))     as leaf_node,
        cast (trim (node_level) as number (15))      as node_level,
        cast (trim (node_1) as varchar2 (60))        as node_1,
        cast (trim (desc_1) as varchar2 (255))       as desc_1,
        cast (trim (node_2) as varchar2 (60))        as node_2,
        cast (trim (desc_2) as varchar2 (255))       as desc_2,
        cast (trim (node_3) as varchar2 (60))        as node_3,
        cast (trim (desc_3) as varchar2 (255))       as desc_3,
        cast (trim (node_4) as varchar2 (60))        as node_4,
        cast (trim (desc_4) as varchar2 (255))       as desc_4,
        cast (trim (node_5) as varchar2 (60))        as node_5,
        cast (trim (desc_5) as varchar2 (255))       as desc_5,
        cast (trim (node_6) as varchar2 (60))        as node_6,
        cast (trim (desc_6) as varchar2 (255))       as desc_6,
        cast (trim (node_7) as varchar2 (60))        as node_7,
        cast (trim (desc_7) as varchar2 (255))       as desc_7,
        cast (trim (node_8) as varchar2 (60))        as node_8,
        cast (trim (desc_8) as varchar2 (255))       as desc_8,
        cast (trim (node_9) as varchar2 (60))        as node_9,
        cast (trim (desc_9) as varchar2 (255))       as desc_9,
        cast (trim (node_10) as varchar2 (60))       as node_10,
        cast (trim (desc_10) as varchar2 (255))      as desc_10,
        cast (trim (node_11) as varchar2 (60))       as node_11,
        cast (trim (desc_11) as varchar2 (255))      as desc_11,
        cast (trim (node_12) as varchar2 (60))       as node_12,
        cast (trim (desc_12) as varchar2 (255))      as desc_12,
        cast (trim (node_13) as varchar2 (60))       as node_13,
        cast (trim (desc_13) as varchar2 (255))      as desc_13,
        cast (trim (node_14) as varchar2 (60))       as node_14,
        cast (trim (desc_14) as varchar2 (255))      as desc_14,
        cast (trim (node_15) as varchar2 (60))       as node_15,
        cast (trim (desc_15) as varchar2 (255))      as desc_15,
        cast (trim (node_16) as varchar2 (60))       as node_16,
        cast (trim (desc_16) as varchar2 (255))      as desc_16,
        cast (trim (node_17) as varchar2 (60))       as node_17,
        cast (trim (desc_17) as varchar2 (255))      as desc_17,
        cast (trim (node_18) as varchar2 (60))       as node_18,
        cast (trim (desc_18) as varchar2 (255))      as desc_18,
        cast (trim (node_19) as varchar2 (60))       as node_19,
        cast (trim (desc_19) as varchar2 (255))      as desc_19,
        cast (trim (node_20) as varchar2 (60))       as node_20,
        cast (trim (desc_20) as varchar2 (255))      as desc_20,
        load_date,
        update_date
    from final