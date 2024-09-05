{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        transient=false,
        tags=["onestream", "rdm", "xml"],
        schema=env_var("DBT_SRC_RAW_DATA_SCHEMA"),
        on_schema_change="sync_all_columns",
        pre_hook=[ 
            "{{ truncate_if_exists(env_var('DBT_SRC_RAW_DATA_SCHEMA'), 'src_ref_onestream_hier_xml') }}",
            "copy into {{ ref('src_ref_onestream_hier_xml') }}
                from @{{ env_var('DBT_SRC_ENT_DB') }}.{{ env_var('DBT_SRC_RAW_DATA_SCHEMA') }}.PHI_DSCI_ONESTREAM_INTERNAL_STAGE
                pattern = '.*/OneStream Production_metatada.xml'
                file_format = (type=XML strip_outer_element=true);"
            ]     
    )
}}
with source as (

    select * from {{ ref('src_ref_onestream_hier_xml') }}

)

, onestream as (
select distinct
---xmldata, --
--dimensions.value as dimensions_value, --

-- dimension element fields
--dimension.value as dimension_value, --

GET(dimension.value, '@name')::string AS dimension_name,
/*
GET(dimension.value, '@type')::string AS dimension_type,
GET(dimension.value, '@accessGroup')::string AS dimension_accessGroup,
GET(dimension.value, '@maintenanceGroup')::string AS dimension_maintenanceGroup,
GET(dimension.value, '@nadescriptionme')::string AS dimension_description,
GET(dimension.value, '@inheritedDim')::string AS dimension_inheritedDim,
GET(dimension.value, '@dimMemberSourceType')::string AS dimension_dimMemberSourceType,
GET(dimension.value, '@dimMemberSourcePath')::string AS dimension_dimMemberSourcePath,
GET(dimension.value, '@dimMemberSourceNVPairs')::string AS dimension_dimMemberSourceNVPairs,
*/
-- member element fields
---member.value as member_value,
/*
GET(member.value, '@name')::string as member_name, --
GET(member.value, '@description')::string as member_description, --
GET(member.value, '@displayMemberGroup')::string as member_displayMemberGroup, --
*/
-- relationships.value as relationships_value, --

-- relationship element fields
-- relationship.value, --
GET(relationship.value, '@aggregationWeight')::integer AS relationship_aggregationWeight,
--GET(relationship.value, '@parent')::string AS relationship_parent,
--GET(relationship.value, '@child')::string AS relationship_child,
case when GET(dimension.value, '@name')::string = 'Random1' and GET(relationship.value, '@parent')::string is null then 'root'
     else GET(relationship.value, '@parent')::string end as relationship_parent,
case when GET(dimension.value, '@name')::string = 'Random1' and GET(relationship.value, '@child')::string is null then 'Pull_Zeroes'
     else GET(relationship.value, '@child')::string end as relationship_child
, seq4() as XPK_METADATAROOT_DIMENSIONS_DI1

from source
  ,  lateral FLATTEN( GET(xmldata, '$') ) dimensions
  ,  lateral FLATTEN( GET(dimensions.value, '$') ) dimension
--  ,  lateral FLATTEN( GET(dimension.value, '$') ) members --
--  ,  lateral FLATTEN( GET(members.value, '$') ) member --
  ,  LATERAL FLATTEN( GET(dimension.value, '$')) relationships 
  ,  LATERAL FLATTEN( GET(relationships.value, '$')) relationship
where GET(dimensions.value, '@') ='dimensions'
  and GET(dimension.value, '@') ='dimension'
--  and GET(members.value, '@') ='members' --
--  and GET(member.value, '@') ='member' --
  AND GET(relationships.value, '@') = 'relationships'
--  AND GET(relationship.value, '@') = 'relationship'
)
, RW as (
select dimension_name, RELATIONSHIP_CHILD, RELATIONSHIP_PARENT, RELATIONSHIP_AGGREGATIONWEIGHT, XPK_METADATAROOT_DIMENSIONS_DI1
, row_number() over (partition by dimension_name, RELATIONSHIP_CHILD, RELATIONSHIP_PARENT, RELATIONSHIP_AGGREGATIONWEIGHT order by XPK_METADATAROOT_DIMENSIONS_DI1) as rw
from onestream
)
, XPK as (
select dimension_name, RELATIONSHIP_CHILD, RELATIONSHIP_PARENT, RELATIONSHIP_AGGREGATIONWEIGHT
, XPK_METADATAROOT_DIMENSIONS_DI1
, seq4() as XPK_METADATAROOT_DIMENSIONS_DI
from RW
WHERE RW = 1
order by to_number(XPK_METADATAROOT_DIMENSIONS_DI1)
)
 , XFK as (
select dimension_name as dimension_name, min(XPK_METADATAROOT_DIMENSIONS_DI) rank from XPK group by dimension_name
)

select 
distinct 
--dimension_name,
cast ( relationship_aggregationWeight as VARCHAR(255) ) as AGGREGATIONWEIGHT,
cast ( relationship_child as VARCHAR(255) ) as child,
cast ( relationship_parent as VARCHAR(255) ) as parent,
DENSE_RANK() over (order by rank)-1  as XFK_METADATAROOT_DIMENSIONS_DI, 

--cast ( DENSE_RANK() OVER ( PARTITION BY RELATIONSHIP_PARENT ORDER BY dimension_name ) as VARCHAR(255) ) as XFK_METADATAROOT_DIMENSIONS_DI, 
cast ( XPK_METADATAROOT_DIMENSIONS_DI as VARCHAR(255) ) as XPK_METADATAROOT_DIMENSIONS_DI

from XPK
inner join XFK on XPK.dimension_name = XFK.dimension_name
order by XPK_METADATAROOT_DIMENSIONS_DI