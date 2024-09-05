{{ config(
    tags=["wbx", "sales","actuals_hist","sales_actuals_hist"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    ) 
}}

/*
TODO:
This model points to the source file created for development purpose. Once real file is received,
source model needs to be modified based on that.

*/


with ibe_history_sales as (select * from {{ ref('src_ibe_history_sales') }})
select
    'WEETABIX' as source_system,
    Sales_Order_Line as sales_line_number,
    Sales_Order as sales_order_number,
    Order_Type as source_sales_order_type,
    'IBE' as sales_order_company,
    cast(null as varchar2) as source_employee_code,
    cast(null as varchar2) as source_line_type_code,
    site as ship_source_business_unit_code, --site is concatenation of ship_source_business_unit_code and source_business_unit_code
    site as source_business_unit_code, --site is concatenation of ship_source_business_unit_code and source_business_unit_code
    Customer_Account as ship_source_customer_code,
    cast(null as varchar2) as bill_source_customer_code,
    sku as source_item_identifier,
    cast(null as varchar2) as org_unit_code,
    cast(null as varchar2) as invoice_document_company,
    cast(null as varchar2) as invoice_document_number,
    cast(null as varchar2) as invoice_document_type,
    cast(null as varchar2) as source_location_code, --populated
    cast(null as varchar2) as source_lot_code, --this field is null in actual code
    cast(null as varchar2) as lot_status_code,
    TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Ordered, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as ordered_date,
    TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Despatched, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as  line_actual_ship_date, --populated
    cast(null as varchar2) as line_sch_pick_up_date,
    cast(null as varchar2) as line_prom_ship_date,
    TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Cancelled, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as line_cancelled_date, --populated
    TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_Invoiced, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as line_invoice_date, --populated
    cast(null as varchar2) as line_requested_date,
    cast(null as varchar2) as line_original_promised_date,
    cast(null as varchar2) as line_promised_delivery_date,
    TO_TIMESTAMP_NTZ(TO_CHAR(TO_TIMESTAMP_NTZ(Date_GL, 'DD-Mon-YY'), 'YYYY-MM-DD HH24:MI:SS.000'))  as line_gl_date, --populated
    cast(null as varchar2)as required_delivery_date,
    cast(null as varchar2) as lead_time,
    cast(null as varchar2) as customer_po_number,
    cast(null as varchar2) as kit_source_item_identifier,
    cast(null as varchar2) as kit_line_number,
    cast(null as varchar2) as kit_component_number,
    cast(null as varchar2) as component_line_no,
    cast(null as varchar2) as price_override_flag,
    cast(null as varchar2) as cost_override_flag,
    Payment_Terms as source_payment_terms_code, --populated
    cast(null as varchar2) as source_freight_handling_code,
    Trans_Unit_Of_Measure as transaction_quantity_uom, --populated
    cast(0 as number(19, 2)) as transaction_price_uom,
    cast(0 as number(19, 2)) as sales_tran_quantity, --this field seems needed
    Qty_Ordered as ordered_tran_quantity, --populated
    Qty_Despatched as shipped_tran_quantity, --populated
    Qty_Cancelled as cancel_tran_quantity, --populated
    Qty_Short as short_tran_quantity, --populated
    cast(0 as number(19, 2)) as backord_tran_quantity,
    Trans_Currency as transaction_currency, --populated
    cast(0 as number(21, 4)) as trans_unit_tran_price,
    cast(0 as number(21, 4)) as trans_list_tran_price,
    cast(0 as number(21, 4)) as trans_extend_tran_price,
    cast(0 as number(21, 4)) as tran_unit_tran_cost,
    cast(0 as number(21, 4)) as trans_extend_tran_cost,
    cast(0 as number(21, 4)) as trans_deduction_01_amt,
    cast(0 as number(38, 10)) as trans_deduction_02_amt,
    cast(0 as number(38, 10)) as trans_deduction_03_amt,
    cast(0 as number(38, 10)) as source_foreign_conv_rt,
    localtimestamp as source_updated_datetime, --added localtimestamp
    cast(null as varchar2) as short_reason_code,
    cast(0 as number(38, 10)) as trans_conv_rt,
    cast(0 as number(38, 10)) as trans_rpt_grs_amt,
    cast(0 as number(38, 10)) as trans_rpt_grs_price,
    cast(0 as number(38, 10)) as trans_rpt_net_amt,
    cast(0 as number(38, 10)) as trans_rpt_net_price,
    Order_Status as line_status_code, --populated
    Qty_Open as open_tran_quantity,  --populated
    cast(0 as number(38, 10)) as source_payment_instr_code,
    null as packingslipid,
    cast(null as varchar2) as edi_indicator,  -- HJ 04 NOV 20,added 'EI' as well to be 1 
    cast(null as varchar2) as cancel_reason_code,
    cast(null as varchar2) as cancel_reason_desc,
    cast(0 as number(19, 2)) as trans_quantity_confirmed,
    cast(0 as number(19, 2)) as trans_salesprice_confirmed,
    cast(0 as number(19, 2)) as trans_lineamount_confirmed,
    null as trans_confirmdate_confirmed,
    cast(0 as number(19, 2)) as trans_uom_confirmed,
    cast(null as varchar2) as trans_currency_confirmed,
    cast(null as varchar2) as source_object_id,
    cast(null as varchar2) as cost_centre,
    Amt_Invoice_Gross as  trans_invoice_grs_amt, --populated
    Amt_Invoice_Net as trans_invoice_net_amt, --populated
    Amt_Invoice_Discount as trans_invoice_disc_amt, --populated
    cast(0 as number(19, 2)) base_invoice_grs_amt,
    cast(0 as number(19, 2)) as base_invoice_net_amt,
    cast(0 as number(19, 2)) as base_invoice_disc_amt,
    cast(null as varchar2) as variant_code,
    cast(null as varchar2) as delivery_instruction,
    cast(null as varchar2) as cust_order_number,
    cast(null as varchar2) as item_shelf_life,
    cast(null as varchar2) as picking_route,
    null as trans_line_requested_date
from ibe_history_sales
