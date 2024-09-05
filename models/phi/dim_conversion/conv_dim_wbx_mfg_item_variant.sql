{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

with old_dim as (
select * from {{ source('WBX_PROD','dim_wbx_mfg_item_variant') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
)

select 
source_system ,
source_item_identifier,
item_guid,
company_code,
variant_code,
variant_desc,
variant_status,
item_allocation_key,
variant_eff_version_no,
effective_date,
expiration_date,
active_flag,
source_updated_date,
load_date,
update_date,
unique_key
from  old_dim 