{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation=ref('conv_sls_wtx_slsorder_fact') %} 

{% set dbt_relation=ref('fct_wbx_sls_order') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 

    exclude_columns=[
        "employee_guid",
        "ship_customer_address_guid",
        "bill_customer_address_guid",
        "ship_business_unit_guid",
        "item_guid",
        "location_guid",
        "lot_guid",
        "business_unit_address_guid",
        "kit_item_guid",
        "payment_terms_guid",
        "account_guid",
        "load_date",
        "update_date",
        "BASE_OPEN_AMT",
        "BASE_RPT_GRS_PRIM_AMT",
        "BASE_RPT_NET_PRIM_AMT",
        "PHI_RPT_GRS_PRIM_AMT",
        "PHI_RPT_NET_PRIM_AMT",
        "PCOMP_RPT_GRS_PRIM_AMT",
        "PCOMP_RPT_NET_PRIM_AMT",
        "PHI_M_GRS_PRIM_AMT",
        "PHI_M_NET_PRIM_AMT",
        "PCOMP_M_GRS_PRIM_AMT",
        "PCOMP_M_NET_PRIM_AMT",
        "TRANS_RPT_GRS_AMT",
        "TRANS_RPT_NET_AMT",
        "TRANS_DEDUCTION_01_AMT",
        "TRANS_DEDUCTION_02_AMT",
        "TRANS_DEDUCTION_03_AMT",
        "BASE_DEDUCTION_01_AMT",
        "BASE_DEDUCTION_02_AMT",
        "BASE_DEDUCTION_03_AMT",
        "PHI_DEDUCTION_01_AMT",
        "PHI_DEDUCTION_02_AMT",
        "PHI_DEDUCTION_03_AMT",
        "PCOMP_DEDUCTION_01_AMT",
        "PCOMP_DEDUCTION_02_AMT",
        "PCOMP_DEDUCTION_03_AMT",
        "TRANS_INVOICE_GRS_AMT",
        "TRANS_INVOICE_NET_AMT",
        "BASE_INVOICE_GRS_AMT",
        "BASE_INVOICE_NET_AMT",
        "PHI_INVOICE_GRS_AMT",
        "PHI_INVOICE_NET_AMT",
        "PCOMP_INVOICE_GRS_AMT",
        "PCOMP_INVOICE_NET_AMT",
        "TRANS_INVOICE_DISC_AMT",
        "BASE_INVOICE_DISC_AMT",
        "PHI_INVOICE_DISC_AMT",
        "PCOMP_INVOICE_DISC_AMT",
        "SHORT_PRIM_QUANTITY",
        "ORDERED_PRIM_QUANTITY",
        "SHIPPED_PRIM_QUANTITY",
        "BACKORDER_PRIM_QUANTITY",
        "CANCELLED_PRIM_QUANTITY",
        "ORDERED_CWT_QUANTITY",
        "SHIPPED_CWT_QUANTITY",
        "BACKORD_CWT_QUANTITY",
        "CANCEL_CWT_QUANTITY",
        "SHORT_CWT_QUANTITY",
        "ORDERED_CA_QUANTITY",
        "SHIPPED_CA_QUANTITY",
        "BACKORD_CA_QUANTITY",
        "CANCEL_CA_QUANTITY",
        "SHORT_CA_QUANTITY",
        "ORDERED_TRAN_QUANTITY",
        "SHIPPED_TRAN_QUANTITY",
        "CANCEL_TRAN_QUANTITY",
        "SHORT_TRAN_QUANTITY",
        "BACKORD_TRAN_QUANTITY",
        "ORDERED_UL_QUANTITY",
        "SHIPPED_UL_QUANTITY",
        "SHORT_UL_QUANTITY",
        "BACKORD_UL_QUANTITY",
        "CANCEL_UL_QUANTITY",
        "ORDERED_KG_QUANTITY",
        "SHIPPED_KG_QUANTITY",
        "CANCEL_KG_QUANTITY",
        "BACKORD_KG_QUANTITY",
        "SHORT_KG_QUANTITY",
        "OPEN_PRIM_QUANTITY",
        "OPEN_CWT_QUANTITY",
        "OPEN_TRAN_QUANTITY",
        "OPEN_UL_QUANTITY",
        "OPEN_KG_QUANTITY",
        "SALES_PRIM_QUANTITY",
        "SALES_CWT_QUANTITY",
        "SALES_CA_QUANTITY",
        "SALES_TRAN_QUANTITY",
        "SALES_UL_QUANTITY",
        "SALES_KG_QUANTITY",
        "TRANS_QUANTITY_CONFIRMED",
        "PRIM_QUANTITY_CONFIRMED",
        "CWT_QUANTITY_CONFIRMED",
        "KG_QUANTITY_CONFIRMED",
        "CA_QUANTITY_CONFIRMED",
        "OPEN_CA_QUANTITY",
        "UL_QUANTITY_CONFIRMED",
        "BASE_LINEAMOUNT_CONFIRMED",
        "PHI_LINEAMOUNT_CONFIRMED",
        "PCOMP_LINEAMOUNT_CONFIRMED",
        "BASE_UNIT_PRIM_COST" ,
        "PHI_M_GRS_PRIM_PRICE" ,
        "PHI_RPT_GRS_KG_PRICE" ,
        "TRAN_UNIT_TRAN_COST"
    ], 

    primary_key="UNIQUE_KEY",
    summarize=true 

) }} 