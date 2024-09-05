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
    prc_po_fact as (select * from {{ ref("fct_wbx_fin_prc_po") }}),
    itm_sales_category_dim as (select * from {{ ref("dim_wbx_sls_item_category") }}),
    itm_procurement_category_dim as (
        select * from {{ ref("dim_wbx_prc_item_category") }}
    ),
    adr_plant_dc_master_dim as (select * from {{ ref("dim_wbx_plant_dc") }}),
    adr_business_rep_dim as (select * from {{ ref("dim_wbx_business_rep") }}),
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
            pcd.master_planning_family_code,  -- ID.ITEM_PRP4_NAME,
            sc.cost_object,  -- PRP5
          --  sc.dimension_group,  -- PRP6
          null as dimension_group,
            itm.formula_variation,  -- DI.ITEM_SRP0_NAME,
       
            sc.profit_loss_code,  -- SRP1
          
          --  sc.plcode_label_owner,  -- SRP2
          null as plcode_label_owner,
            sc.freight_handling,  -- SRP3
         
           sc.consumer_unit_size,  -- .ITEM_SRP4_NAME,
           
           sc.default_broker_comm_rate,  -- SRP5
          
            sc.label_owner,  -- SRP6
          
           sc.manufacturer_id,  -- SRP7
        
            sc.sales_catergory5_code,  -- SRP8
          
            pcd.master_reporting_category,
            pcd.alternate_reporting_category,
            sup.supplier_name,
          coalesce(catd.supplier_type, sup.phi_supplier_type) as phi_supplier_type,
          
            coalesce(catd.category_level1, sup.phi_supplier_subtype) as phi_supplier_subtype,
         
            coalesce(
                catd.unified_supplier_name, sup.unified_supplier_name
            ) as unified_supplier_name,
           
            buyer.representative_name buyer_name,
            buyer.source_system_address_number buyer_number,
            pdcm.source_business_unit_code,
            pdcm.business_unit_name,
            act.account_type,
            act.account_description,
            act.source_concat_nat_key,
            contract_agreement_flag,
            contract_company_code,
            contract_line_number,
            contract_number,
            contract_type,
            freight_handling_code_desc,
            gl_offset_srccd,
            line_onhold_quantity,
            line_open_quantity,
            line_order_quantity,
            line_recvd_quantity,
            po.line_status,
            po.line_type,
            po.base_line_unit_cost,
            po.txn_line_unit_cost,
            po.phi_line_unit_cost,
            po.pcomp_line_unit_cost,
            pay.payment_terms_code,
            pay.payment_terms_description,
            phi_conv_rt,
            phi_currency,
            phi_line_on_hold_amt,
            phi_line_open_amt,
            phi_line_received_amt,
            phi_order_total_amount,
            po_cancelled_date,
            po_delivery_date,
            po_gl_date,
            po_line_desc,
            po_line_number,
            po_order_company,
            po_order_date,
            po_order_number,
            po_order_suffix,
            po_order_type,
            po_promised_delivery_date,
            po_requested_date,
            po.caf_no,
            act.source_account_identifier,
            source_buyer_identifier,
            source_contract_type,
            source_freight_handling_code,
            itm.source_item_identifier,
            po.source_payment_terms_code,
            source_po_order_type,
          --  sub.source_subledger_identifier,
          null as source_subledger_identifier,
          --  sub.subledger_type,
          null as subledger_type,
            po.source_supplier_identifier,
            po.source_system,
          --  subledger_type_desc target_account_identifier,
          null as target_account_identifier,
            target_freight_handling_code,
            transaction_uom,
            txn_conv_rt,
            txn_currency,
            txn_line_on_hold_amt,
            txn_line_open_amt,
            txn_line_received_amt,
            txn_order_total_amount,
            base_currency,
            base_line_on_hold_amt,
            base_line_open_amt,
            base_line_received_amt,
            base_order_total_amount,
            pcomp_order_total_amount,
            pcomp_line_on_hold_amt,
            pcomp_line_open_amt,
            pcomp_line_received_amt,
            pdcm.operating_company,
            po_org_promise_date,
            itm.max_reorder_quantity,
            itm.min_reorder_quantity,
            itm.multiple_order_quantity,
       
            itm.reorder_point,
        
            itm.reorder_quantity,
         
            pcd.lead_time,
            pcd.safety_stock,
        
            catd.category_level2 as phi_supplier_speciality,
 
    
           catd.category_level3 phi_spend_profile,
        
            catd.category_level4 phi_sourcing_agent,
        
            sup.source_supplier_type source_supplier_type
        from itm_item_master_dim itm
        right join
            prc_po_fact po
            on itm.item_guid = po.item_guid
            and itm.business_unit_address_guid = po.business_unit_address_guid
            and itm.source_system = po.source_system
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
            on pdcm.plantdc_address_guid = po.business_unit_address_guid
            and pdcm.source_system = po.source_system
        left join
            adr_business_rep_dim buyer
            on buyer.rep_address_number_guid = po.buyer_address_number_guid
            and po.source_system = buyer.source_system
        left join
            fin_account_dim act
            on po.account_guid = act.account_guid
            and po.source_system = act.source_system
        left join
            adr_supplier_master_dim sup
            on po.supplier_address_number_guid = sup.supplier_address_number_guid
        left join
            ref_payment_terms_xref pay
            on po.payment_terms_guid = pay.payment_terms_guid
            and po.source_system = pay.source_system
       /* left join
            fin_subledger_dim sub
            on po.subledger_guid = sub.subledger_guid
            and po.source_system = sub.source_system*/
        left join
            adr_supplier_category_dim catd
            on catd.source_system = po.source_system
            and catd.source_system_address_number = sup.source_system_address_number
        where po.source_system = '{{ env_var("DBT_SOURCE_SYSTEM") }}'
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
    unified_supplier_name,
    buyer_name,
    buyer_number,
    source_business_unit_code,
    business_unit_name,
    account_type,
    account_description,
    source_concat_nat_key,
    contract_agreement_flag,
    contract_company_code,
    contract_line_number,
    contract_number,
    contract_type,
    freight_handling_code_desc,
    gl_offset_srccd,
    line_onhold_quantity,
    line_open_quantity,
    line_order_quantity,
    line_recvd_quantity,
    line_status,
    line_type,
    base_line_unit_cost,
    txn_line_unit_cost,
    phi_line_unit_cost,
    pcomp_line_unit_cost,
    payment_terms_code,
    payment_terms_description,
    phi_conv_rt,
    phi_currency,
    phi_line_on_hold_amt,
    phi_line_open_amt,
    phi_line_received_amt,
    phi_order_total_amount,
    po_cancelled_date,
    po_delivery_date,
    po_gl_date,
    po_line_desc,
    po_line_number,
    po_order_company,
    po_order_date,
    po_order_number,
    po_order_suffix,
    po_order_type,
    po_promised_delivery_date,
    po_requested_date,
    caf_no,
    source_account_identifier,
    source_buyer_identifier,
    source_contract_type,
    source_freight_handling_code,
    source_item_identifier,
    source_payment_terms_code,
    source_po_order_type,
    source_subledger_identifier,
    subledger_type,
    source_supplier_identifier,
    source_system,
    target_account_identifier,
    target_freight_handling_code,
    transaction_uom,
    txn_conv_rt,
    txn_currency,
    txn_line_on_hold_amt,
    txn_line_open_amt,
    txn_line_received_amt,
    txn_order_total_amount,
    base_currency,
    base_line_on_hold_amt,
    base_line_open_amt,
    base_line_received_amt,
    base_order_total_amount,
    pcomp_order_total_amount,
    pcomp_line_on_hold_amt,
    pcomp_line_open_amt,
    pcomp_line_received_amt,
    operating_company,
    po_org_promise_date,
    max_reorder_quantity,
    min_reorder_quantity,
    multiple_order_quantity,
    reorder_point,
    reorder_quantity,
    lead_time,
    safety_stock,
    phi_supplier_speciality,
  
     phi_spend_profile,
    
    phi_sourcing_agent,
  
    source_supplier_type
from source
