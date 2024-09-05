{{
    config(
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags = ["wbx","sales","sales_actuals","actuals"],
    )
}}
with slsorder_fact as (
    select * from {{ ref('fct_wbx_sls_order')}}
),
sls_order_hdr_fact as (
    select * from {{ ref('fct_wbx_sls_order_hdr')}}
),
plant_dc as (
    select * from {{ ref('dim_wbx_plant_dc')}} 
),
address as (
    select * from {{ ref('dim_wbx_address')}}
),
customer as (
    select * from {{ ref('dim_wbx_customer')}}
),
cust_master_ext as (
    select * from {{ ref('dim_wbx_customer_ext')}}
),
ref_hierarchy_xref as (
    select * from {{ ref('xref_wbx_hierarchy')}}
),
item_master as (
    select * from {{ ref('dim_wbx_item')}}
),
item_master_ext as (
    select * from {{ ref('dim_wbx_item_ext')}}
),
final as (
    select
        sls_line.source_system,
        sls_line.sales_line_number,
        sls_line.sales_order_number,
        sls_line.source_sales_order_type,
        sls_line.sales_order_type,
        sls_line.sales_order_company,
        sls_line.source_employee_code,
        sls_line.line_type_code,
        sls_line.line_type_desc,
        sls_line.line_status_code,
        sls_line.line_status_desc,
        sls_line.ship_source_customer_code,
        sls_line.bill_source_customer_code,
        sls_line.source_item_identifier,
        sls_line.invoice_document_company,
        sls_line.invoice_document_number,
        sls_line.invoice_document_type,
        sls_line.source_lot_code,
        sls_line.line_ordered_date,
        sls_line.line_sch_pick_up_date,
        sls_line.line_prom_ship_date,
        sls_line.line_cancelled_date,
        sls_line.line_invoice_date,
        sls_line.line_requested_date,
        sls_line.line_original_promised_date,
        sls_line.line_promised_delivery_date,
        sls_line.line_gl_date,
        sls_line.required_delivery_date,
        sls_line.lead_time,
        sls_line.early_flag,
        sls_line.late_flag,
        sls_line.source_business_unit_code,
        sls_line.customer_po_number,
        sls_line.open_ca_quantity,
        sls_line.base_open_amt,
        sls_line.source_payment_terms_code,
        sls_line.source_payment_instr_code,
        sls_line.target_payment_instr_code,
        sls_line.payment_instr_desc,
        sls_line.source_freight_handling_code,
        sls_line.source_freight_handling_desc,
        sls_line.basket_price_code,
        sls_line.transaction_quantity_uom,
        sls_line.primary_uom,
        sls_line.transaction_price_uom,
        sls_line.unit_weight,
        sls_line.weight_uom,
        sls_line.unit_volume,
        sls_line.volume_uom,
        sls_line.gross_weight_uom,
        sls_line.net_kg_weight,
        sls_line.gross_kg_weight,
        sls_line.delivery_instruction,
        sls_line.cust_order_number,
        sls_line.item_shelf_life,
        sls_line.picking_route,
        sls_line.trans_line_requested_date,
        sls_line.short_prim_quantity,
        sls_line.ordered_prim_quantity,
        sls_line.shipped_prim_quantity,
        sls_line.backorder_prim_quantity,
        sls_line.cancelled_prim_quantity,
        sls_line.ordered_ca_quantity,
        sls_line.shipped_ca_quantity,
        sls_line.backord_ca_quantity,
        sls_line.cancel_ca_quantity,
        sls_line.short_ca_quantity,
        sls_line.ordered_tran_quantity,
        sls_line.shipped_tran_quantity,
        sls_line.cancel_tran_quantity,
        sls_line.short_tran_quantity,
        sls_line.backord_tran_quantity,
        sls_line.ordered_ul_quantity,
        sls_line.shipped_ul_quantity,
        sls_line.short_ul_quantity,
        sls_line.backord_ul_quantity,
        sls_line.cancel_ul_quantity,
        sls_line.ordered_kg_quantity,
        sls_line.shipped_kg_quantity,
        sls_line.cancel_kg_quantity,
        sls_line.backord_kg_quantity,
        sls_line.short_kg_quantity,
        sls_line.base_currency,
        sls_line.phi_currency,
        sls_line.phi_conv_rt,
        sls_line.mth_phi_conv_rt,
        sls_line.pcomp_currency,
        sls_line.pcomp_conv_rt,
        sls_line.mth_pcomp_conv_rt,
        sls_line.base_unit_prim_price,
        sls_line.base_list_prim_price,
        sls_line.base_extend_tran_price,
        sls_line.base_unit_prim_cost,
        sls_line.base_extend_tran_cost,
        sls_line.base_rpt_grs_prim_price,
        sls_line.base_rpt_grs_prim_amt,
        sls_line.base_rpt_net_prim_price,
        sls_line.base_rpt_net_prim_amt,
        sls_line.base_ext_ing_cost as base_ext_ing_cost,
        sls_line.base_ext_pkg_cost,
        sls_line.base_ext_oth_cost,
        sls_line.phi_rpt_grs_prim_amt,
        sls_line.phi_rpt_grs_prim_price,
        sls_line.phi_rpt_net_prim_amt,
        sls_line.phi_rpt_net_prim_price,
        sls_line.pcomp_rpt_grs_prim_amt,
        sls_line.pcomp_rpt_grs_prim_price,
        sls_line.pcomp_rpt_net_prim_amt,
        sls_line.pcomp_rpt_net_prim_price,
        sls_line.phi_m_grs_prim_amt,
        sls_line.phi_m_grs_prim_price,
        sls_line.phi_m_net_prim_amt,
        sls_line.phi_m_net_prim_price,
        sls_line.pcomp_m_grs_prim_amt,
        sls_line.pcomp_m_grs_prim_price,
        sls_line.pcomp_m_net_prim_amt,
        sls_line.pcomp_m_net_prim_price,
        sls_line.base_rpt_grs_kg_price,
        sls_line.base_rpt_net_kg_price,
        sls_line.base_rpt_grs_ca_price,
        sls_line.base_rpt_net_ca_price,
        sls_line.phi_rpt_grs_kg_price,
        sls_line.phi_rpt_net_kg_price,
        sls_line.phi_rpt_grs_ca_price,
        sls_line.phi_rpt_net_ca_price,
        sls_line.pcomp_rpt_grs_kg_price,
        sls_line.pcomp_rpt_net_kg_price,
        sls_line.pcomp_rpt_grs_ca_price,
        sls_line.pcomp_rpt_net_ca_price,
        sls_line.source_foreign_conv_rt,
        sls_line.source_updated_d_id,
        sls_line.source_updated_time,
        sls_line.short_reason_code,
        sls_line.short_reason_desc,
        sls_line.load_date,
        sls_line.update_date,
        sls_line.transaction_currency,
        sls_line.trans_conv_rt,
        sls_line.trans_rpt_grs_amt,
        sls_line.trans_rpt_grs_price,
        sls_line.trans_rpt_net_amt,
        sls_line.trans_rpt_net_price,
        sls_line.open_prim_quantity,
        sls_line.open_tran_quantity,
        sls_line.open_ul_quantity,
        sls_line.open_kg_quantity,
        sls_line.mth_trans_conv_rt,
        sls_line.trans_unit_tran_price,
        sls_line.trans_list_tran_price,
        sls_line.trans_extend_tran_price,
        sls_line.tran_unit_tran_cost,
        sls_line.trans_extend_tran_cost,
        sls_line.trans_deduction_01_amt,
        sls_line.trans_deduction_02_amt,
        sls_line.trans_deduction_03_amt,
        sls_line.sales_prim_quantity,
        sls_line.sales_ca_quantity,
        sls_line.sales_tran_quantity,
        sls_line.sales_ul_quantity,
        sls_line.sales_kg_quantity,
        sls_line.line_actual_ship_date,
        sls_line.edi_indicator,
        sls_line.base_deduction_01_amt,
        sls_line.base_deduction_02_amt,
        sls_line.base_deduction_03_amt,
        sls_line.cancel_reason_code,
        sls_line.cancel_reason_desc,
        sls_line.trans_quantity_confirmed,
        sls_line.trans_salesprice_confirmed,
        sls_line.trans_lineamount_confirmed,
        sls_line.trans_confirmdate_confirmed,
        sls_line.trans_uom_confirmed,
        sls_line.trans_currency_confirmed,
        sls_line.prim_quantity_confirmed,
        sls_line.cwt_quantity_confirmed,
        sls_line.ul_quantity_confirmed,
        sls_line.kg_quantity_confirmed,
        sls_line.ca_quantity_confirmed,
        sls_line.base_lineamount_confirmed,
        sls_line.phi_lineamount_confirmed,
        sls_line.pcomp_lineamount_confirmed,
        sls_line.base_ext_boughtin_cost,
        sls_line.base_ext_copack_cost,
        sls_line.source_account_identifier,
        sls_line.source_object_id,
        sls_line.cost_centre,
        sls_line.account_guid,
        sls_line.trans_invoice_grs_amt,
        sls_line.trans_invoice_net_amt,
        sls_line.base_invoice_grs_amt,
        sls_line.base_invoice_net_amt,
        sls_line.phi_invoice_grs_amt,
        sls_line.phi_invoice_net_amt,
        sls_line.pcomp_invoice_grs_amt,
        sls_line.pcomp_invoice_net_amt,
        sls_line.variant_code,
        sls_line.base_ext_lbr_cost,
        sls_hdr.source_base_currency,
        sls_hdr.ordered_date,
        sls_hdr.sched_pick_date,
        sls_hdr.cancelled_date,
        sls_hdr.invoice_date,
        sls_hdr.requested_date,
        sls_hdr.actual_ship_date,
        sls_hdr.arrival_date,
        sls_hdr.revised_crad_date,
        sls_hdr.crad_date,
        sls_hdr.hold_status,
        sls_hdr.header_status_code,
        sls_hdr.header_status_desc,
        bu.business_unit_name,
        cust_st.source_system_address_number as ship_source_system_address_number,
        cust_st.company_name as ship_company_name,
        cust_st.customer_name as ship_customer_name,
        cust_st.customer_type as ship_customer_type,
      --cust_st.customer_type_description as ship_customer_type_description,
        null as ship_customer_type_description,
        cust_st.bill_to as ship_bill_to,
        cust_st.currency_code as ship_currency_code,
        cust_st.payment_terms_code as ship_payment_terms_code,
        cust_st.payment_terms_description as ship_payment_terms_description,
        cust_st.csr_address_number as ship_csr_address_number,
      --cust_st.sales_rep_address_number as ship_sales_rep_address_number,
        null as ship_sales_rep_address_number,
        cust_st.unified_customer as ship_unified_customer,
      --cust_st.shipping_method as ship_shipping_method,
        '' as ship_shipping_method,
        cust_st.shipping_terms as ship_shipping_terms,
        cust_st.customer_group as ship_customer_group,
      --cust_st.bill_name as ship_bill_name,
        null as ship_bill_name,
        cust_st.customer_group_name as ship_customer_group_name,
        cust_st.csr_name as ship_csr_name,
      --cust_st.sales_rep_type as ship_sales_rep_type,
        null as ship_sales_rep_type,
      --cust_st.sales_rep_name as ship_sales_rep_name,
        null as ship_sales_rep_name,
        cust_st.legacy_customer_number as ship_legacy_customer_number,
        nvl(cust_st.company_code, '') as company_code,
        nvl(cust_st.company_name, '') as company_name,
        nvl(cust_st.customer_name, '') as customer_name,
        nvl(cust_st.customer_type, '') as customer_type,
        nvl(cust_st.currency_code, '') as currency_code,
        nvl(cust_st.unified_customer, '') as unified_customer,
        nvl(cust_st.customer_group, '') as customer_group,
        nvl(cust_st.customer_group_name, '') as customer_group_name,
        nvl(cust_st.csr_name, '') as csr_name,
        cust_bt.source_system_address_number as bill_source_system_address_number,
        cust_bt.customer_name as bill_customer_name,
        cust_bt.customer_type as bill_customer_type,
        cust_bt.bill_to as bill_bill_to,
        cust_bt.currency_code as bill_currency_code,
        cust_bt.payment_terms_code as bill_payment_terms_code,
        cust_bt.payment_terms_description as bill_payment_terms_description,
      --cust_bt.sales_rep_address_number as bill_sales_rep_address_number,
        null as bill_sales_rep_address_number,
      --cust_bt.shipping_method as bill_shipping_method,
        null as bill_shipping_method,
        cust_bt.shipping_terms as bill_shipping_terms,
        cust_bt.customer_group as bill_customer_group,
      --cust_bt.bill_name as bill_bill_name,
        null as bill_bill_name,
        cust_bt.customer_group_name as bill_customer_group_name,
      --cust_bt.sales_rep_type as bill_sales_rep_type,
        null as bill_sales_rep_type,
      --cust_bt.sales_rep_name as bill_sales_rep_name,
        null as bill_sales_rep_name,
        cdh_st.trade_sector_desc,
        cdh_st.market_code_seq as market_seq,
        cdh_st.sub_market_code_seq as sub_market_seq,
        lpad(cdh_st.trade_class_seq, 4, 0) as trade_class_seq,
        lpad(cdh_st.trade_group_seq, 4, 0) as trade_group_seq,
        lpad(cdh_st.trade_type_seq, 3, 0) as trade_type_seq,
        lpad(cdh_st.trade_sector_seq, 3, 0) as trade_sector_seq,
        cdh_st.price_group,
        cdh_st.total_so_qty_discount,
        cdh_st.additional_discount,
        cdh_st.customer_rebate_group,
        cdh_st.fin_dim_customer as customer,
        cus_h_st.desc_1 as market,
        cus_h_st.desc_2 as sub_market,
        cus_h_st.desc_3 as trade_class,
        cus_h_st.desc_4 as trade_group,
        cus_h_st.desc_5 as trade_type,
        cus_h_st.desc_6 as customer_account,
        cus_h_st.desc_7 as customer_branch,
        st_adr.address_line_1 as ship_address_line_1,
        st_adr.postal_code as ship_postal_code,
        st_adr.city as ship_city,
        st_adr.state_province as ship_state_province,
        st_adr.country as ship_country,
        bt_adr.address_line_1 as bill_address_line_1,
        bt_adr.postal_code as bill_postal_code,
        bt_adr.city as bill_city,
        bt_adr.state_province as bill_state_province,
        bt_adr.country as bill_country,
      --item.brand_code,
        null as brand_code,
      --item.brand_name,
        null as brand_name,
        item.buyer_code,
        item.case_gross_weight,
        item.case_item_number,
        item.case_net_weight,
      --item.case_upc,
        null as case_upc,
      --item.consumer_gtin_number,
        null as consumer_gtin_number,
      --item.consumer_unit_size,
        null as consumer_unit_size,
      --item.consumer_units_per_case,
        null as consumer_units_per_case,
      --item.consumer_upc,
        null as consumer_upc,
        item.description,
        item.item_type,
      --item.planner_code,
        null as planner_code,
        item.primary_uom as item_primary_uom,
        item.short_description,
        item.stock_type,
        nvl(item.stock_desc, '') as stock_desc,
        nvl(item.primary_uom_desc, '') as primary_uom_desc,
        nvl(item.obsolete_flag, '') as obsolete_flag,
        pdh.strategic_desc,
        pdh.manufacturing_group_desc,
        pdh.pack_size_desc,
        pdh.category_desc,
        pdh.sub_category_desc,
        pdh.promo_type_desc,
        pdh.net_weight,
        pdh.gross_weight,
        pdh.tare_weight,
        pdh.avp_weight,
        pdh.avp_flag,
        pdh.consumer_units_in_trade_units,
        pdh.consumer_units,
        pdh.pallet_qty,
        pdh.pallet_qty_per_layer,
        pdh.current_flag,
        pdh.exclude_indicator,
        pdh.power_brand_desc,
        pdh.mangrpcd_site,
        pdh.mangrpcd_plant,
        pdh.mangrpcd_copack_flag,
        pdh.gross_depth,
        pdh.gross_width,
        pdh.gross_height,
        pdh.branding_seq,
        lpad(pdh.product_class_seq, 4, 0) as product_class_seq,
        lpad(pdh.sub_product_seq, 4, 0) as sub_product_seq,
        lpad(pdh.strategic_seq, 4, 0) as strategic_seq,
        lpad(pdh.power_brand_seq, 3, 0) as power_brand_seq,
        lpad(pdh.manufacturing_group_seq, 3, 0) as manufacturing_group_seq,
        lpad(pdh.pack_size_seq, 5, 0) as pack_size_seq,
        pdh.category_seq,
        lpad(pdh.promo_type_seq, 2, 0) as promo_type_seq,
        pdh.sub_category_seq,
        prod_h.desc_1 as branding,
        prod_h.desc_2 as product_class,
        prod_h.desc_3 as sub_product,
        prod_h.desc_4 as item_sku

    from slsorder_fact sls_line
    left join
        sls_order_hdr_fact sls_hdr
        on sls_line.sales_order_number = sls_hdr.sales_order_number
        and sls_line.source_sales_order_type = sls_hdr.source_sales_order_type
        and sls_line.sales_order_company = sls_hdr.sales_order_company

    left join
        plant_dc bu
        on bu.source_system = sls_line.source_system
        and bu.plantdc_address_guid = sls_line.business_unit_address_guid

    left join
        address st_adr
        on sls_hdr.ship_customer_addr_number_guid = st_adr.address_guid

    left join
        address bt_adr
        on sls_hdr.bill_customer_addr_number_guid = bt_adr.address_guid

    left join
        customer cust_st
        on cust_st.customer_address_number_guid = sls_line.ship_customer_address_guid

    left join
        cust_master_ext cdh_st
        on sls_hdr.ship_customer_addr_number_guid = cdh_st.customer_address_number_guid

    left join
        ref_hierarchy_xref cus_h_st
        on sls_line.source_system = cus_h_st.source_system
        and sls_line.ship_source_customer_code = cus_h_st.node_7
        and cus_h_st.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        and cus_h_st.hier_name = 'CUSTOMER-SALES'
        and cus_h_st.company_code=sls_line.sales_order_company

    left join
        customer cust_bt
        on cust_bt.customer_address_number_guid = sls_line.bill_customer_address_guid

    left join
        item_master item
        on item.source_system = sls_line.source_system
        and item.item_guid = sls_line.item_guid
        and item.source_business_unit_code = sls_line.ship_source_business_unit_code

    left join
        item_master_ext pdh
        on pdh.source_system = sls_line.source_system
        and pdh.item_guid = sls_line.item_guid
        and pdh.source_business_unit_code = sls_line.ship_source_business_unit_code

    left join
        ref_hierarchy_xref prod_h
        on sls_line.source_system = prod_h.source_system
        and sls_line.source_item_identifier = prod_h.node_4
        and prod_h.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
        and prod_h.hier_name = 'ITEM-SALES'
)
select
    source_system,
    sales_line_number,
    sales_order_number,
    source_sales_order_type,
    sales_order_type,
    sales_order_company,
    source_employee_code,
    line_type_code,
    line_type_desc,
    line_status_code,
    line_status_desc,
    ship_source_customer_code,
    bill_source_customer_code,
    source_item_identifier,
    invoice_document_company,
    invoice_document_number,
    invoice_document_type,
    source_lot_code,
    line_ordered_date,
    line_sch_pick_up_date,
    line_prom_ship_date,
    line_cancelled_date,
    line_invoice_date,
    line_requested_date,
    line_original_promised_date,
    line_promised_delivery_date,
    line_gl_date,
    required_delivery_date,
    lead_time,
    early_flag,
    late_flag,
    source_business_unit_code,
    customer_po_number,
    open_ca_quantity,
    base_open_amt,
    source_payment_terms_code,
    source_payment_instr_code,
    target_payment_instr_code,
    payment_instr_desc,
    source_freight_handling_code,
    source_freight_handling_desc,
    basket_price_code,
    transaction_quantity_uom,
    primary_uom,
    transaction_price_uom,
    unit_weight,
    weight_uom,
    unit_volume,
    volume_uom,
    gross_weight_uom,
    net_kg_weight,
    gross_kg_weight,
    delivery_instruction,
    cust_order_number,
    item_shelf_life,
    picking_route,
    trans_line_requested_date,
    short_prim_quantity,
    ordered_prim_quantity,
    shipped_prim_quantity,
    backorder_prim_quantity,
    cancelled_prim_quantity,
    ordered_ca_quantity,
    shipped_ca_quantity,
    backord_ca_quantity,
    cancel_ca_quantity,
    short_ca_quantity,
    ordered_tran_quantity,
    shipped_tran_quantity,
    cancel_tran_quantity,
    short_tran_quantity,
    backord_tran_quantity,
    ordered_ul_quantity,
    shipped_ul_quantity,
    short_ul_quantity,
    backord_ul_quantity,
    cancel_ul_quantity,
    ordered_kg_quantity,
    shipped_kg_quantity,
    cancel_kg_quantity,
    backord_kg_quantity,
    short_kg_quantity,
    base_currency,
    phi_currency,
    phi_conv_rt,
    mth_phi_conv_rt,
    pcomp_currency,
    pcomp_conv_rt,
    mth_pcomp_conv_rt,
    base_unit_prim_price,
    base_list_prim_price,
    base_extend_tran_price,
    base_unit_prim_cost,
    base_extend_tran_cost,
    base_rpt_grs_prim_price,
    base_rpt_grs_prim_amt,
    base_rpt_net_prim_price,
    base_rpt_net_prim_amt,
    base_ext_ing_cost,
    base_ext_pkg_cost,
    base_ext_oth_cost,
    phi_rpt_grs_prim_amt,
    phi_rpt_grs_prim_price,
    phi_rpt_net_prim_amt,
    phi_rpt_net_prim_price,
    pcomp_rpt_grs_prim_amt,
    pcomp_rpt_grs_prim_price,
    pcomp_rpt_net_prim_amt,
    pcomp_rpt_net_prim_price,
    phi_m_grs_prim_amt,
    phi_m_grs_prim_price,
    phi_m_net_prim_amt,
    phi_m_net_prim_price,
    pcomp_m_grs_prim_amt,
    pcomp_m_grs_prim_price,
    pcomp_m_net_prim_amt,
    pcomp_m_net_prim_price,
    base_rpt_grs_kg_price,
    base_rpt_net_kg_price,
    base_rpt_grs_ca_price,
    base_rpt_net_ca_price,
    phi_rpt_grs_kg_price,
    phi_rpt_net_kg_price,
    phi_rpt_grs_ca_price,
    phi_rpt_net_ca_price,
    pcomp_rpt_grs_kg_price,
    pcomp_rpt_net_kg_price,
    pcomp_rpt_grs_ca_price,
    pcomp_rpt_net_ca_price,
    source_foreign_conv_rt,
    source_updated_d_id,
    source_updated_time,
    short_reason_code,
    short_reason_desc,
    load_date,
    update_date,
    transaction_currency,
    trans_conv_rt,
    trans_rpt_grs_amt,
    trans_rpt_grs_price,
    trans_rpt_net_amt,
    trans_rpt_net_price,
    open_prim_quantity,
    open_tran_quantity,
    open_ul_quantity,
    open_kg_quantity,
    mth_trans_conv_rt,
    trans_unit_tran_price,
    trans_list_tran_price,
    trans_extend_tran_price,
    tran_unit_tran_cost,
    trans_extend_tran_cost,
    trans_deduction_01_amt,
    trans_deduction_02_amt,
    trans_deduction_03_amt,
    sales_prim_quantity,
    sales_ca_quantity,
    sales_tran_quantity,
    sales_ul_quantity,
    sales_kg_quantity,
    line_actual_ship_date,
    edi_indicator,
    base_deduction_01_amt,
    base_deduction_02_amt,
    base_deduction_03_amt,
    cancel_reason_code,
    cancel_reason_desc,
    trans_quantity_confirmed,
    trans_salesprice_confirmed,
    trans_lineamount_confirmed,
    trans_confirmdate_confirmed,
    trans_uom_confirmed,
    trans_currency_confirmed,
    prim_quantity_confirmed,
    cwt_quantity_confirmed,
    ul_quantity_confirmed,
    kg_quantity_confirmed,
    ca_quantity_confirmed,
    base_lineamount_confirmed,
    phi_lineamount_confirmed,
    pcomp_lineamount_confirmed,
    base_ext_boughtin_cost,
    base_ext_copack_cost,
    source_account_identifier,
    source_object_id,
    cost_centre,
    account_guid,
    trans_invoice_grs_amt,
    trans_invoice_net_amt,
    base_invoice_grs_amt,
    base_invoice_net_amt,
    phi_invoice_grs_amt,
    phi_invoice_net_amt,
    pcomp_invoice_grs_amt,
    pcomp_invoice_net_amt,
    variant_code,
    base_ext_lbr_cost,
    source_base_currency,
    ordered_date,
    sched_pick_date,
    cancelled_date,
    invoice_date,
    requested_date,
    actual_ship_date,
    arrival_date,
    revised_crad_date,
    crad_date,
    hold_status,
    header_status_code,
    header_status_desc,
    business_unit_name,
    ship_source_system_address_number,
    ship_company_name,
    ship_customer_name,
    ship_customer_type,
    ship_customer_type_description,
    ship_bill_to,
    ship_currency_code,
    ship_payment_terms_code,
    ship_payment_terms_description,
    ship_csr_address_number,
    ship_sales_rep_address_number,
    ship_unified_customer,
    ship_shipping_method,
    ship_shipping_terms,
    ship_customer_group,
    ship_bill_name,
    ship_customer_group_name,
    ship_csr_name,
    ship_sales_rep_type,
    ship_sales_rep_name,
    ship_legacy_customer_number,
    company_code,
    company_name,
    customer_name,
    customer_type,
    currency_code,
    unified_customer,
    customer_group,
    customer_group_name,
    csr_name,
    bill_source_system_address_number,
    bill_customer_name,
    bill_customer_type,
    bill_bill_to,
    bill_currency_code,
    bill_payment_terms_code,
    bill_payment_terms_description,
    bill_sales_rep_address_number,
    bill_shipping_method,
    bill_shipping_terms,
    bill_customer_group,
    bill_bill_name,
    bill_customer_group_name,
    bill_sales_rep_type,
    bill_sales_rep_name,
    trade_sector_desc,
    market_seq,
    sub_market_seq,
    trade_class_seq,
    trade_group_seq,
    trade_type_seq,
    trade_sector_seq,
    price_group,
    total_so_qty_discount,
    additional_discount,
    customer_rebate_group,
    customer,
    market,
    sub_market,
    trade_class,
    trade_group,
    trade_type,
    customer_account,
    customer_branch,
    ship_address_line_1,
    ship_postal_code,
    ship_city,
    ship_state_province,
    ship_country,
    bill_address_line_1,
    bill_postal_code,
    bill_city,
    bill_state_province,
    bill_country,
    brand_code,
    brand_name,
    buyer_code,
    case_gross_weight,
    case_item_number,
    case_net_weight,
    case_upc,
    consumer_gtin_number,
    consumer_unit_size,
    consumer_units_per_case,
    consumer_upc,
    description,
    item_type,
    planner_code,
    item_primary_uom,
    short_description,
    stock_type,
    stock_desc,
    primary_uom_desc,
    obsolete_flag,
    strategic_desc,
    manufacturing_group_desc,
    pack_size_desc,
    category_desc,
    sub_category_desc,
    promo_type_desc,
    net_weight,
    gross_weight,
    tare_weight,
    avp_weight,
    avp_flag,
    consumer_units_in_trade_units,
    consumer_units,
    pallet_qty,
    pallet_qty_per_layer,
    current_flag,
    exclude_indicator,
    power_brand_desc,
    mangrpcd_site,
    mangrpcd_plant,
    mangrpcd_copack_flag,
    gross_depth,
    gross_width,
    gross_height,
    branding_seq,
    product_class_seq,
    sub_product_seq,
    strategic_seq,
    power_brand_seq,
    manufacturing_group_seq,
    pack_size_seq,
    category_seq,
    promo_type_seq,
    sub_category_seq,
    branding,
    product_class,
    sub_product,
    item_sku
from final