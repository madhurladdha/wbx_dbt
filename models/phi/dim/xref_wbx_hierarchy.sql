{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    transient = false,
    on_schema_change='sync_all_columns'
    )
}}

with int_hier_onestream_wbx_account_src as (
    select * from {{ ref('stg_d_wbx_account_hier_onestream') }}
),

int_hier_onestream_item_src as (
    select * from {{ ref('stg_d_wbx_item_hier_onestream') }}
),

int_hier_onestream_cust_src as (
    select * from {{ ref('stg_d_wbx_customer_hier_onestream') }}
),

int_hier_onestream_cc_src as (
    select * from {{ ref('stg_d_wbx_cc_hier_onestream') }}
),

int_hier_onestream_account_src as (
    select * from {{ ref('stg_d_wbx_account_onestream') }}
),

History as 
(
    select * from {{ ref('conv_ref_hierarchy_xref') }} where 1=2
),



Final as
(
    
    select 
    SOURCE_SYSTEM,
    '-' as company_code,
    HIER_CATEGORY,
    HIER_NAME,
    tagetik_account,
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
    DESC_7,
    NODE_8,
    DESC_8,
    NODE_9,
    DESC_9,
    NODE_10,
    DESC_10,
    NODE_11,
    DESC_11,
    NODE_12,
    DESC_12,
    NODE_13,
    DESC_13,
    NODE_14,
    DESC_14,
    NODE_15,
    DESC_15,
    NODE_16,
    DESC_16,
    NODE_17,
    DESC_17,
    NODE_18,
    DESC_18,
    NODE_19,
    DESC_19,
    NODE_20,
    DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from int_hier_onestream_account_src
union
   select 
    SOURCE_SYSTEM,
    '-' as company_code,
    HIER_CATEGORY,
    HIER_NAME,
    tagetik_account,
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
    DESC_7,
    NODE_8,
    DESC_8,
    NODE_9,
    DESC_9,
    NODE_10,
    DESC_10,
    NODE_11,
    DESC_11,
    NODE_12,
    DESC_12,
    NODE_13,
    DESC_13,
    NODE_14,
    DESC_14,
    NODE_15,
    DESC_15,
    NODE_16,
    DESC_16,
    NODE_17,
    DESC_17,
    NODE_18,
    DESC_18,
    NODE_19,
    DESC_19,
    NODE_20,
    DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from int_hier_onestream_item_src
union
    
   select 
    SOURCE_SYSTEM,
    '-' as company_code,
    HIER_CATEGORY,
    HIER_NAME,
    tagetik_account,
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
    DESC_7,
    NODE_8,
    DESC_8,
    NODE_9,
    DESC_9,
    NODE_10,
    DESC_10,
    NODE_11,
    DESC_11,
    NODE_12,
    DESC_12,
    NODE_13,
    DESC_13,
    NODE_14,
    DESC_14,
    NODE_15,
    DESC_15,
    NODE_16,
    DESC_16,
    NODE_17,
    DESC_17,
    NODE_18,
    DESC_18,
    NODE_19,
    DESC_19,
    NODE_20,
    DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from int_hier_onestream_cc_src
union
   select 
    SOURCE_SYSTEM,
    company_code,
    HIER_CATEGORY,
    HIER_NAME,
    tagetik_account,
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
    DESC_7,
    NODE_8,
    DESC_8,
    NODE_9,
    DESC_9,
    NODE_10,
    DESC_10,
    NODE_11,
    DESC_11,
    NODE_12,
    DESC_12,
    NODE_13,
    DESC_13,
    NODE_14,
    DESC_14,
    NODE_15,
    DESC_15,
    NODE_16,
    DESC_16,
    NODE_17,
    DESC_17,
    NODE_18,
    DESC_18,
    NODE_19,
    DESC_19,
    NODE_20,
    DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from int_hier_onestream_cust_src
union

  select 
    SOURCE_SYSTEM,
    '-' as company_code,
    HIER_CATEGORY,
    HIER_NAME,
    tagetik_account,
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
    DESC_7,
    NODE_8,
    DESC_8,
    NODE_9,
    DESC_9,
    NODE_10,
    DESC_10,
    NODE_11,
    DESC_11,
    NODE_12,
    DESC_12,
    NODE_13,
    DESC_13,
    NODE_14,
    DESC_14,
    NODE_15,
    DESC_15,
    NODE_16,
    DESC_16,
    NODE_17,
    DESC_17,
    NODE_18,
    DESC_18,
    NODE_19,
    DESC_19,
    NODE_20,
    DESC_20, 
    LOAD_DATE,
    UPDATE_DATE
    from int_hier_onestream_wbx_account_src
  
)

select * from Final