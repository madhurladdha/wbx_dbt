{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_prc_supplier_categorization') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_prc_supplier_categorization'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["ADDRESS_LINE_2",
"SUPPLIER_SUBTYPE",
"D&B_SIC_DESCRIPTION1",
"D&B_SIC_DESCRIPTION2",
"D&B_SIC_DESCRIPTION3",
"D&B_SIC_DESCRIPTION4",
"D&B_SIC_DESCRIPTION5",
"D&B_SIC_DESCRIPTION6",
"NAICS_DESCRIPTION1",
"NAICS_DESCRIPTION2",
"NAICS_DESCRIPTION3",
"NAICS_DESCRIPTION4",
"NAICS_DESCRIPTION5",
"NAICS_DESCRIPTION6",
"CERTIFIED_SMALLBUSINESS",
"DISABLED_VET_BUS_ENTERPRISE",
"DISADVANTAGED_BUS_ENTERPRISE",
"LABOR_SURPLUS_AREA",
"MINORITY_BUS_ENTERPRISE",
"SMALL_BUSINESS_INDICATOR",
"SMALL_DISADVANTAGED_BUS",
"VETERAN_OWNED_INDICATOR",
"WOMAN-OWND_BUS_ENTERPRSE",
"WOMAN-OWNED_INDICATOR",
"ALTERNATE_SUPPLIER_NUMBER",
"ALASKAN_NATIVE_CORPORATION",
"DISADVNTGED_VET_ENTERPRISE",
"HIST_BLK_COLL-UNIV_MIN_INSTITN",
"HUB-ZONE_CERTIFICATION",
"MINORITY_CERTIFICATION_CODE",
"SERVICE_DISABLED_VET_OWNED",
"VETERAN_BUS_ENTERPRISE",
"VIETNAM_VETERAN_OWNED",
"DATE_INSERTED",
"DATE_UPDATED"], 
primary_key="UNIQUE_KEY",
summarize=false
) }}