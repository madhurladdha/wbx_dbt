{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_sls_item_category') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_sls_item_category'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["update_date","load_date","sales_catergory1_code","ADESSO_CATEGORY","sales_catergory2_code", "sales_catergory3_code", "sales_catergory4_code", "sales_catergory5_code", "label_owner", "manufacturer_id", "customer_selling_unit", "case_markings_flag", "consumer_package_type", "consumer_unit_size", "freight_handling", "organic", "gmo", "cost_object", "dimension_group", "manufacturing_technology", "reporting_segment", "reporting_seg_sub_group", "mixed_pallet_mod_indicator", "salvage_requirement", "profit_loss_code", "plcode_label_owner", "default_broker_comm_rate", "default_bu", "cost_object_desc", "pasta_cut", "pasta_cut_desc", "pasta_cut_length", "semo_type_desc", "semo_additive_desc", "pasta_shape_desc"],
    summarize=false

) }} 

