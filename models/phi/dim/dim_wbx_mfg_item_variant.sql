{{
    config(
    tags=["manufacturing","supply_schedule","item_variant","dim","wbx"],
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    full_refresh=false)
}}

with old_table as
(
    select * from {{ref('conv_dim_wbx_mfg_item_variant')}}  
    /*{% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if  model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 model is present.

{% endif %}*/

/*commenting above "if" condition as this will prevent history data load in BR2 */

),

base_fct  as (
    select * from {{ref('int_d_wbx_mfg_item_variant')}}
    qualify row_number() over(partition by unique_key order by ITEM_ALLOCATION_KEY )=1
   /* {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %} */
),
old_model as (
select 
	cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(substring(company_code,1,255) as text(255) ) as company_code  ,

    cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

    cast(substring(variant_desc,1,510) as text(510) ) as variant_desc  ,

    cast(variant_status as number(10,0) ) as variant_status  ,

    cast(substring(item_allocation_key,1,255) as text(255) ) as item_allocation_key  ,

    cast(variant_eff_version_no as number(10,0) ) as variant_eff_version_no  ,

    cast(effective_date as timestamp_ntz(9) ) as effective_date  ,

    cast(expiration_date as timestamp_ntz(9) ) as expiration_date  ,

    cast(substring(active_flag,1,255) as text(255) ) as active_flag  ,

    cast(source_updated_date as timestamp_ntz(9) ) as source_updated_date  ,

    cast(load_date as timestamp_ntz(9) ) as load_date  ,

    cast(update_date as timestamp_ntz(9) ) as update_date,
    cast(unique_key as text(255) )                                  as unique_key
from old_table
),
snpt_fact as (
    select 
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(substring(company_code,1,255) as text(255) ) as company_code  ,

    cast(substring(variant_code,1,255) as text(255) ) as variant_code  ,

    cast(substring(variant_desc,1,510) as text(510) ) as variant_desc  ,

    cast(variant_status as number(10,0) ) as variant_status  ,

    cast(substring(item_allocation_key,1,255) as text(255) ) as item_allocation_key  ,

    cast(variant_eff_version_no as number(10,0) ) as variant_eff_version_no  ,

    cast(effective_date as timestamp_ntz(9) ) as effective_date  ,

    cast(expiration_date as timestamp_ntz(9) ) as expiration_date  ,

    cast(substring(active_flag,1,255) as text(255) ) as active_flag  ,

    cast(source_updated_date as timestamp_ntz(9) ) as source_updated_date  ,

    cast(load_date as timestamp_ntz(9) ) as load_date  ,

    cast(update_date as timestamp_ntz(9) ) as update_date,
    cast(unique_key as text(255) )                                  as unique_key
from base_fct bf 
)
select * from snpt_fact
union
select * from old_model