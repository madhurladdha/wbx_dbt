{{
    config(
        materialized=env_var("DBT_MAT_VIEW"),
        tags=["finance", "po"],
        transient=false,
        on_schema_change="sync_all_columns",
    )
}}
with 
    itm_item_master_dim as (select * from {{ ref("dim_wbx_item") }}),
    prc_po_receipt_fact as (select * from {{ ref("fct_wbx_fin_prc_po_receipt") }}),
    itm_sales_category_dim as (select * from {{ ref("dim_wbx_sls_item_category") }}),
    itm_procurement_category_dim as (
        select * from {{ ref("dim_wbx_prc_item_category") }}
    ),
    adr_plant_dc_master_dim as (select * from {{ ref("dim_wbx_plant_dc") }}),
    fin_account_dim as (select * from {{ ref("dim_wbx_account") }}),
    adr_supplier_master_dim as (select * from {{ ref("dim_wbx_supplier") }}),
    ref_payment_terms_xref as (select * from {{ ref("xref_wbx_payment_terms") }}),
    -- fin_subledger_dim as (select * from {{ ref("xref_wbx_payment_terms") }}),
    adr_supplier_category_dim as (select * from {{ ref("src_dim_supplier_category") }}),

    source as (
        select
            itm.item_type,
            itm.case_upc,
           
            itm.legacy_case_item_number,
            itm.buyer_code,
            itm.planner_code,
            
            itm.case_net_weight,
            itm.case_gross_weight,
            itm.primary_uom item_primary_uom,
           itm.purchase_make_indicator,
           itm.description,
            itm.pack_size_desc,
            itm.case_item_number,
            itm.short_description,
            itm.gl_class_name,
          
            itm.consumer_gtin_number,
          
            sc.customer_selling_unit,  -- PRP0
          
            pcd.highlevel_category_code,  -- ID.Z_PRP1_NA,
            sc.sales_catergory1_code,  -- PRP2
         
            sc.sales_catergory3_code,  -- PRP3
            
            pcd.master_planning_family_code  -- ID.ITEM_PRP4_NAME,
           as master_planning_family_code,
            sc.cost_object,  -- PRP5
          
            sc.dimension_group,  -- PRP6
          
            itm.formula_variation,  -- DI.ITEM_SRP0_NAME,
           
            sc.profit_loss_code,  -- SRP1
           
            sc.plcode_label_owner,  -- SRP2
            
             sc.freight_handling,  -- SRP3
            
            sc.consumer_unit_size,  -- .ITEM_SRP4_NAME,
          
            sc.default_broker_comm_rate,  -- SRP5
          
            sc.label_owner,  -- SRP6
         
            sc.manufacturer_id,  -- SRP7
        
            sc.sales_catergory5_code,  -- SRP8
            
            pcd.master_reporting_category,
            pcd.alternate_reporting_category,
            sup.supplier_name,
           coalesce(catd.supplier_type, sup.phi_supplier_type)
           as phi_supplier_type,
            coalesce(catd.category_level1, sup.phi_supplier_subtype)
           as phi_supplier_subtype,
            pdcm.source_business_unit_code,
            pdcm.business_unit_name,
            act.account_type,
            act.account_description,
            act.source_concat_nat_key,
            gl_offset_srccd,
            pay.payment_terms_code,
            pay.payment_terms_description,
            transaction_uom,
            txn_currency,
            phi_currency,
            pcomp_currency,
            base_currency,
            supplier_invoice_number,
           -- subledger_type_desc,
            null as subledger_type_desc,
            por.source_system,
            source_supplier_identifier,
            por.source_payment_terms_code,
            por.source_item_identifier,
            receipt_stocked_quantity,
            receipt_scrapped_quantity,
            receipt_reworked_quantity,
            receipt_returned_quantity,
            receipt_rejected_quantity,
            receipt_received_quantity,
            txn_receipt_received_amt,
            phi_receipt_received_amt,
            pcomp_receipt_received_amt,
            base_receipt_received_amt,
            receipt_paidtodate_quantity,
            txn_receipt_paidtodate_amt,
            phi_receipt_paidtodate_amt,
            pcomp_receipt_paidtodate_amt,
            base_receipt_paidtodate_amt,
            receipt_order_quantity,
            receipt_open_quantity,
            txn_receipt_open_amt,
            phi_receipt_open_amt,
            pcomp_receipt_open_amt,
            base_receipt_open_amt receipt_closed_quantity,
            txn_receipt_closed_amt,
            phi_receipt_closed_amt,
            pcomp_receipt_closed_amt,
            base_receipt_closed_amt,
            base_receipt_open_amt,
            receipt_adjusted_quantity,
            po_requested_date,
            po_received_date,
            po_receipt_match_type,
            po_promised_dlv_date,
            po_order_type,
            po_order_suffix,
            po_order_number,
            po_order_date,
            po_order_company,
            po_number_of_lines,
            po_line_number,
            pay_status_code,
            por.line_type,
            line_status,
            gl_date,
            document_type,
            document_pay_item,
            document_number,
            document_company,
            itm.max_reorder_quantity,
            itm.min_reorder_quantity,
            itm.multiple_order_quantity,
          
            itm.reorder_point,
           
            itm.reorder_quantity,
            
            pcd.lead_time,
           pcd.safety_stock as safety_stock,
            coalesce(catd.unified_supplier_name, sup.unified_supplier_name)
           as unified_supplier_name,
            por.base_receipt_unit_cost,
            por.txn_receipt_unit_cost,
            por.phi_receipt_unit_cost,
            por.pcomp_receipt_unit_cost,
            catd.category_level2            as phi_supplier_speciality,
            catd.category_level3             as phi_spend_profile, 
            catd.category_level4             as phi_sourcing_agent

      
        -- CONV_STATUS
        -- CONV_NOTES
        from itm_item_master_dim itm
        right join
            prc_po_receipt_fact por
            on itm.item_guid = por.item_guid
            and itm.business_unit_address_guid = por.business_unit_address_guid
            and itm.source_system = por.source_system
        left join
            itm_sales_category_dim sc
            on itm.item_guid = sc.item_guid
            and itm.business_unit_address_guid = sc.business_unit_address_guid
            and itm.source_system = sc.source_system
        left join
            itm_procurement_category_dim pcd
            on itm.item_guid = pcd.item_guid
            and itm.business_unit_address_guid = pcd.business_unit_address_guid
            and itm.source_system = pcd.source_system
        left join
            adr_plant_dc_master_dim pdcm
            on pdcm.plantdc_address_guid = por.business_unit_address_guid
            and pdcm.source_system = por.source_system
        left join
            fin_account_dim act
            on por.account_guid = act.account_guid
            and por.source_system = act.source_system
        left join
            adr_supplier_master_dim sup
            on por.supplier_address_number_guid = sup.supplier_address_number_guid
        left join
            ref_payment_terms_xref pay
            on por.payment_terms_guid = pay.payment_terms_guid
            and por.source_system = pay.source_system
       /* left join
            fin_subledger_dim sub
            on por.subledger_guid = sub.subledger_guid
            and por.source_system = sub.source_system*/
        left join
            adr_supplier_category_dim catd
            on catd.source_system = por.source_system
            and catd.source_system_address_number = sup.source_system_address_number
        where por.source_system = '{{ env_var("DBT_SOURCE_SYSTEM") }}'
    )

select
    item_type,
    case_upc,
    legacy_case_item_number,
    buyer_code,
    planner_code,
    case_net_weight,
    case_gross_weight,
    item_primary_uom,
    purchase_make_indicator,
    description,
    pack_size_desc,
    case_item_number,
    short_description,
    gl_class_name,
    consumer_gtin_number,
    customer_selling_unit,
    highlevel_category_code,
    sales_catergory1_code,
    sales_catergory3_code,
    master_planning_family_code,
    cost_object,
    dimension_group,
    formula_variation,
    profit_loss_code,
    plcode_label_owner,
    freight_handling,
    consumer_unit_size,
    default_broker_comm_rate,
    label_owner,
    manufacturer_id,
    sales_catergory5_code,
    master_reporting_category,
    alternate_reporting_category,
    supplier_name,
    phi_supplier_type,
    phi_supplier_subtype,
    source_business_unit_code,
    business_unit_name,
    account_type,
    account_description,
    source_concat_nat_key,
    gl_offset_srccd,
    payment_terms_code,
    payment_terms_description,
    transaction_uom,
    txn_currency,
    phi_currency,
    pcomp_currency,
    base_currency,
    supplier_invoice_number,
    subledger_type_desc,
    source_system,
    source_supplier_identifier,
    source_payment_terms_code,
    source_item_identifier,
    receipt_stocked_quantity,
    receipt_scrapped_quantity,
    receipt_reworked_quantity,
    receipt_returned_quantity,
    receipt_rejected_quantity,
    receipt_received_quantity,
    txn_receipt_received_amt,
    phi_receipt_received_amt,
    pcomp_receipt_received_amt,
    base_receipt_received_amt,
    receipt_paidtodate_quantity,
    txn_receipt_paidtodate_amt,
    phi_receipt_paidtodate_amt,
    pcomp_receipt_paidtodate_amt,
    base_receipt_paidtodate_amt,
    receipt_order_quantity,
    receipt_open_quantity,
    txn_receipt_open_amt,
    phi_receipt_open_amt,
    pcomp_receipt_open_amt,
    receipt_closed_quantity,
    txn_receipt_closed_amt,
    phi_receipt_closed_amt,
    pcomp_receipt_closed_amt,
    base_receipt_closed_amt,
    base_receipt_open_amt,
    receipt_adjusted_quantity,
    po_requested_date,
    po_received_date,
    po_receipt_match_type,
    po_promised_dlv_date,
    po_order_type,
    po_order_suffix,
    po_order_number,
    po_order_date,
    po_order_company,
    po_number_of_lines,
    po_line_number,
    pay_status_code,
    line_type,
    line_status,
    gl_date,
    document_type,
    document_pay_item,
    document_number,
    document_company,
    max_reorder_quantity,
    min_reorder_quantity,
    multiple_order_quantity,
    reorder_point,
    reorder_quantity,
    lead_time,
    safety_stock,
    unified_supplier_name,
    base_receipt_unit_cost,
    txn_receipt_unit_cost,
    phi_receipt_unit_cost,
    pcomp_receipt_unit_cost,
    phi_supplier_speciality,
    
    phi_spend_profile,
  
    phi_sourcing_agent
  
from source
