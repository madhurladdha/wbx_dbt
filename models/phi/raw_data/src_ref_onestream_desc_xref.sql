{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        transient=false,
        tags=["onestream", "rdm", "xml"],
        schema=env_var("DBT_SRC_RAW_DATA_SCHEMA"),
        on_schema_change="sync_all_columns",
        pre_hook=[ 
            "{{ truncate_if_exists(env_var('DBT_SRC_RAW_DATA_SCHEMA'), 'src_ref_onstream_desc_xml') }}",
            "copy into {{ ref('src_ref_onstream_desc_xml') }}
                from @{{ env_var('DBT_SRC_ENT_DB') }}.{{ env_var('DBT_SRC_RAW_DATA_SCHEMA') }}.PHI_DSCI_ONESTREAM_INTERNAL_STAGE
                pattern = '.*/OneStream Production_metatada.xml'
                file_format = (type=XML strip_outer_element=true);"
            ]     
    )
}}
with source as (

    select * from {{ ref('src_ref_onstream_desc_xml') }}

)

, onestream as (
select distinct
--GET(member.value, '@name')::string as member_name, 
--GET(member.value, '@description')::string as member_description,
case when GET(dimension.value, '@name')::string = 'Random1' and GET(member.value, '@name')::string is null then 'Pull_Zeroes'
     else GET(member.value, '@name')::string end as member_name,
case when GET(dimension.value, '@name')::string = 'Random1' and GET(member.value, '@description')::string is null then 'Pull Zeroes'
     else GET(member.value, '@description')::string end as member_description
from source
  ,  lateral FLATTEN( GET(xmldata, '$') ) dimensions
  ,  lateral FLATTEN( GET(dimensions.value, '$') ) dimension
  ,  lateral FLATTEN( GET(dimension.value, '$') ) members
  ,  lateral FLATTEN( GET(members.value, '$') ) member
where GET(dimensions.value, '@') ='dimensions'
  and GET(dimension.value, '@') ='dimension'
  and GET(members.value, '@') ='members'
 -- and GET(member.value, '@') ='member'
)
select 
distinct 
cast ( member_description as VARCHAR(255) ) as description,
cast ( member_name as VARCHAR(30) ) as name
from onestream