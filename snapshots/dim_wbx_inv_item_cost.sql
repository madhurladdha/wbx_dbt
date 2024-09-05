{% snapshot dim_wbx_inv_item_cost %} 

 

/* unique_key has been generated by using the surrogate key function and the following source fields in the indicated order: 
source_item_identifier','source_business_unit_code','source_location_code','source_lot_code','source_cost_method_code','variant_code'
*/ 

{{ 
    config( 
      target_schema=env_var('DBT_DIM_SCHEMA'), 
      unique_key='unique_key', 
      strategy='check', 
      check_cols=['ITEM_UNIT_COST'], 
      transient=false,
      tags=["inventory", "item_cost","inv_item_cost"]
    ) 
}} 

 ---------------only using ITEM_unit_cost as check columns (as same is used in  IICS logic)

select * from {{ ref('int_d_wbx_inv_item_cost') }} 

{% endsnapshot %} 