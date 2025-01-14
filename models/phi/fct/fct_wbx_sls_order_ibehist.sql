{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["wbx","sales", "actuals_hist","sales_actuals_hist"],
        transient=false,
        snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
        unique_key="UNIQUE_KEY",
        on_schema_change="sync_all_columns",
        pre_hook="""
            {{ truncate_if_exists(this.schema, this.table) }}
            """
    )
}}

with int_view_f as (
    select * from {{ ref('int_f_wbx_sls_order_ibehist') }}
),

int_view_v as (
    select
        source_system,
        sales_line_number,
        sales_order_number,
        source_sales_order_type,
        sales_order_type,
        sales_order_company,
        line_invoice_date,
        line_actual_ship_date,
        base_ext_ing_cost,
        base_ext_pkg_cost,
        base_ext_lbr_cost,
        base_ext_bought_in_cost,
        base_ext_oth_cost,
        base_ext_copack_cost
    from {{ ref('int_v_wtx_mfg_cbom_variant') }}
    qualify row_number() over (
                    partition by
                        source_system,
                        sales_line_number,
                        sales_order_number,
                        source_sales_order_type,
                        sales_order_type,
                        sales_order_company,
                        line_invoice_date,
                        line_actual_ship_date
                    order by 1) =1 

),

fact as (
    select 
        tgt.source_system,
        tgt.sales_line_number,
        tgt.sales_order_number,
        tgt.source_sales_order_type,
        tgt.sales_order_type,
        tgt.sales_order_company,
        source_employee_code,
        employee_guid,
        source_line_type_code,
        line_type_code,
        line_type_desc,
        line_status_code,
        line_status_desc,
        ship_source_customer_code,
        ship_customer_address_guid,
        bill_source_customer_code,
        bill_customer_address_guid,
        ship_source_business_unit_code,
        ship_business_unit_guid,
        default_company_code,
        source_item_identifier,
        item_guid,
        division,
        org_unit_code,
        invoice_document_company,
        invoice_document_number,
        invoice_document_type,
        source_location_code,
        location_guid,
        source_lot_code,
        lot_guid,
        lot_status_code,
        line_ordered_date,
        line_sch_pick_up_date,
        line_prom_ship_date,
        line_cancelled_date,
        tgt.line_invoice_date,
        line_requested_date,
        line_original_promised_date,
        line_promised_delivery_date,
        line_gl_date,
        required_delivery_date,
        lead_time,
        gl_offset_code,
        early_flag,
        late_flag,
        source_business_unit_code,
        business_unit_address_guid,
        original_document_company,
        original_document_number,
        original_document_type,
        original_line_number,
        transfer_po_order_company,
        transfer_po_order_number,
        transfer_po_order_type,
        transfer_po_line_number,
        customer_po_number,
        vendor_ref_code,
        kit_source_item_identifier,
        kit_item_guid,
        kit_line_number,
        kit_component_number,
        component_line_no,
        base_open_amt,
        price_override_flag,
        cost_override_flag,
        source_payment_terms_code,
        payment_terms_guid,
        source_payment_instr_code,
        target_payment_instr_code,
        payment_instr_desc,
        price_adj_schd_code,
        item_price_grp_code,
        pick_slip_no,
        source_freight_handling_code,
        source_freight_handling_desc,
        serial_no,
        basket_price_code,
        transaction_quantity_uom,
        primary_uom,
        transaction_price_uom,
        unit_weight,
        weight_uom,
        unit_volume,
        volume_uom,
        gross_weight,
        gross_weight_uom,
        net_weight,
        net_kg_weight,
        gross_kg_weight,
        short_prim_quantity,
        ordered_prim_quantity,
        shipped_prim_quantity,
        backorder_prim_quantity,
        cancelled_prim_quantity,
        ordered_cwt_quantity,
        shipped_cwt_quantity,
        backord_cwt_quantity,
        cancel_cwt_quantity,
        short_cwt_quantity,
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
        division_currency,
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
        bracket_price,
        list_price_uom,
        load_date,
        update_date,
        transaction_currency,
        trans_conv_rt,
        trans_rpt_grs_amt,
        trans_rpt_grs_price,
        trans_rpt_net_amt,
        trans_rpt_net_price,
        line_item_desc,
        open_prim_quantity,
        open_cwt_quantity,
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
        sales_cwt_quantity,
        sales_ca_quantity,
        sales_tran_quantity,
        sales_ul_quantity,
        sales_kg_quantity,
        tgt.line_actual_ship_date,
        edi_indicator,
        base_deduction_01_amt,
        base_deduction_02_amt,
        base_deduction_03_amt,
        phi_deduction_01_amt,
        phi_deduction_02_amt,
        phi_deduction_03_amt,
        pcomp_deduction_01_amt,
        pcomp_deduction_02_amt,
        pcomp_deduction_03_amt,
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
        open_ca_quantity,
        src.base_ext_ing_cost,
        src.base_ext_pkg_cost,
        src.base_ext_lbr_cost,
        src.base_ext_bought_in_cost as base_ext_boughtin_cost,
        src.base_ext_oth_cost,
        src.base_ext_copack_cost,
        trans_invoice_disc_amt,
        base_invoice_disc_amt,
        phi_invoice_disc_amt,
        pcomp_invoice_disc_amt,
        delivery_instruction,
        cust_order_number,
        item_shelf_life,
        picking_route,
        trans_line_requested_date,
        unique_key
    from int_view_f tgt
    left join int_view_v src
        on tgt.source_system = src.source_system
        and tgt.sales_line_number = src.sales_line_number
        and tgt.sales_order_number = src.sales_order_number
        and tgt.source_sales_order_type = src.source_sales_order_type
        and case
            when tgt.sales_order_type is null or trim(tgt.sales_order_type) = ''
            then '-'
            else tgt.sales_order_type
        end = case
            when src.sales_order_type is null or trim(tgt.sales_order_type) = ''
            then '-'
            else src.sales_order_type
        end
        and tgt.sales_order_company = src.sales_order_company
        and case
            when tgt.line_invoice_date is null
            then '9999-12-01'
            else tgt.line_invoice_date
        end = case
            when src.line_invoice_date is null then '9999-12-01' else src.line_invoice_date
        end
        and case
            when tgt.line_actual_ship_date is null
            then '9999-12-01'
            else tgt.line_actual_ship_date
        end = case
            when src.line_actual_ship_date is null
            then '9999-12-01'
            else src.line_actual_ship_date
        end

)

select * from fact
qualify row_number() over (partition by unique_key order by unique_key desc)=1