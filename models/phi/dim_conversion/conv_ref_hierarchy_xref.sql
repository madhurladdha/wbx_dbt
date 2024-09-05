    {{
    config(
    materialized = 'view',
    )
}}

WITH old_dim AS 
        (
            SELECT * FROM {{source('EI_RDM','ref_hierarchy_xref')}}  where source_system in ('WEETABIX','TAGETIK_ACCOUNT','ONESTREAM_ENTITY','EBITDA_HIER') and  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
        ),

converted_dim AS
(
    select
        source_system,
        tagetik_account,
        node_level,
        node_1,
        desc_1,
        node_2,
        desc_2,
        node_3,
        desc_3,
        node_4,
        desc_4,
        node_5,
        desc_5,
        node_6,
        desc_6,
        node_7,
        desc_7,
        node_8,
        desc_8,
        node_9,
        desc_9,
        node_10,
        desc_10,
        load_date,
        update_date,
        leaf_node,
        hier_category,
        hier_name,
        node_11,
        desc_11,
        node_12,
        desc_12,
        node_13,
        desc_13,
        node_14,
        desc_14,
        node_15,
        desc_15,
        node_16,
        desc_16,
        node_17,
        desc_17,
        node_18,
        desc_18,
        node_19,
        desc_19,
        node_20,
        desc_20
    FROM old_dim
)

select * from converted_dim