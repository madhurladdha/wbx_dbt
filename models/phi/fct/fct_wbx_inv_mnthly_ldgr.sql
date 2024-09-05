{{
    config(
        materialized = env_var('DBT_MAT_INCREMENTAL'),
        tags=["inventory", "inv_monthly_ledger"],
        snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
        transient=false,
        unique_key="UNIQUE_KEY",
        pre_hook="""
    
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
                truncate table {{ this }}
            {% endif %}  

            """,
    )
}}
--this model does not require a conversion model for D365 since it is completely downstream from the Transaction Ledger, which has already been
--converted. We are truncating prior to every reload.

--troubleshoot monthly ledger,

with
    src as (
        select *, row_number() over (partition by unique_key order by 1) rownum
        from {{ ref("int_f_wbx_inv_mnthly_ldgr") }}
    )

    select

        source_system,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        source_location_code,
        location_guid,
        source_lot_code,
        lot_guid,
        fiscal_period_number,
        transaction_currency,
        transaction_uom,
        ledger_qty,
        ledger_amt,
        beginning_inventory_qty,
        beginning_inventory_amt,
        ending_inventory_qty,
        ending_inventory_amt,
        po_receipt_qty,
        po_receipt_amt,
        transfer_in_intercompany_qty,
        transfer_in_intercompany_amt,
        transfer_out_intercompany_qty,
        transfer_out_intercompany_amt,
        transfer_in_intracompany_qty,
        transfer_in_intracompany_amt,
        transfer_out_intracompany_qty,
        transfer_out_intracompany_amt,
        load_date,
        update_date,
        source_updated_d_id,
        beginning_inventory_kg_qty,
        ending_inventory_kg_qty,
        po_receipt_kg_qty,
        ledger_kg_qty,
        unique_key
from src
where rownum = 1