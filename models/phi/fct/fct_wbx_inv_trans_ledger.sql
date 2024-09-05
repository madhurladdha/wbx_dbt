{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    tags=["inventory", "trans_ledger"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    transient=false,
    unique_key="unique_key",
    on_schema_change="sync_all_columns",
    pre_hook="""

        {% if check_table_exists( this.schema, this.table ) == 'True' %}
             truncate table {{ this }}
        {% endif %} 

        """,
     )
}}

--can update snowflake warehouse to medium (ONLY IF RUNNING FULL SET, and use sparingly IF NECESSARY)
/* The fact refresh has to be a FULL REFRESH (not incremental) due to the 
    complexities around the natural key and unique key.  There are risks of breaking the 
    uniqueness and either overcounting or overwriting rows.  An incremental 
    approach may be feasible if the filter is applied to the correct table and tested,
    but the risk is not being taken at this time.
*/


with int_fact as (
    select *
    from {{ ref("int_f_wbx_inv_trans_ledger") }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

old_ax_fact as (
    select * from {{ ref('conv_fct_wbx_inv_trans_ledger') }}
),



int as ( --need to make sure the report columns are in the SAME ORDER
     select
        related_address_number,
        address_guid,
        document_type,
        original_document_type,
        gl_date,
        document_number,
        original_document_number,
        source_item_identifier,
        item_guid,
        document_company,
        original_document_company,
        original_line_number,
        line_number,
        source_location_code,
        location_guid,
        source_lot_code,
        lot_guid,
        lot_status_code,
        lot_status_desc,
        source_business_unit_code,
        business_unit_address_guid,
        transaction_amt,
        reason_code,
        transaction_date,
        remark_txt,
        transaction_qty,
        transaction_uom,
        transaction_unit_cost,
        transaction_currency,
        transaction_pri_uom_qty,
        transaction_pri_uom_unit_cost,
        transaction_pri_uom_amt,
        base_currency,
        base_amt,
        source_system,
        source_document_type,
        source_original_document_type,
        reason_code_desc,
        load_date,
        update_date,
        transaction_kg_qty,
        transaction_lb_qty,
        source_pallet_id,
        variant,
        pallet_count,
        unique_key,
        'D365' as source_legacy
from int_fact),

ax_hist as (
    select
         a.related_address_number,
         a.address_guid,
         a.document_type,
         a.original_document_type,
         a.gl_date,
         a.document_number,
         a.original_document_number,
         a.source_item_identifier,
         a.item_guid,
         a.document_company,
         a.original_document_company,
         a.original_line_number,
         a.line_number,
         a.source_location_code,
         a.location_guid,
         a.source_lot_code,
         a.lot_guid,
         a.lot_status_code,
         a.lot_status_desc,
         a.source_business_unit_code,
         a.business_unit_address_guid,
         a.transaction_amt,
         a.reason_code,
         a.transaction_date,
         a.remark_txt,
         a.transaction_qty,
         a.transaction_uom,
         a.transaction_unit_cost,
         a.transaction_currency,
         a.transaction_pri_uom_qty,
         a.transaction_pri_uom_unit_cost,
         a.transaction_pri_uom_amt,
         a.base_currency,
         a.base_amt,
         a.source_system,
         a.source_document_type,
         a.source_original_document_type,
         a.reason_code_desc,
         a.load_date,
         a.update_date,
         a.transaction_kg_qty,
         a.transaction_lb_qty,
         a.source_pallet_id,
         a.variant,
         a.pallet_count,
         a.unique_key,
         'AX' as source_legacy
    from old_ax_fact as a
),

final as (
    select * from int
    union
    select * from ax_hist
)

select
    cast(substring(related_address_number, 1, 255) as varchar(255))
        as related_address_number,
    cast(substring(address_guid, 1, 255) as varchar(255)) as address_guid,
    cast(substring(document_type, 1, 20) as varchar(20)) as document_type,
    cast(substring(original_document_type, 1, 20) as varchar(20))
        as original_document_type,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(substring(document_number, 1, 255) as varchar(255))
        as document_number,
    cast(substring(original_document_number, 1, 255) as varchar(255))
        as original_document_number,
    cast(substring(source_item_identifier, 1, 255) as varchar(255))
        as source_item_identifier,
    cast(substring(item_guid, 1, 255) as varchar(255)) as item_guid,
    cast(substring(document_company, 1, 20) as varchar(20))
        as document_company,
    cast(substring(original_document_company, 1, 20) as varchar(20))
        as original_document_company,
    cast(original_line_number as number(38, 10)) as original_line_number,
    cast(line_number as number(38, 10)) as line_number,
    cast(substring(source_location_code, 1, 255) as varchar(255))
        as source_location_code,
    cast(substring(location_guid, 1, 255) as varchar(255))
        as location_guid,
    cast(substring(source_lot_code, 1, 255) as varchar(255))
        as source_lot_code,
    cast(substring(lot_guid, 1, 255) as varchar(255)) as lot_guid,
    cast(substring(lot_status_code, 1, 255) as varchar(255))
        as lot_status_code,
    cast(substring(lot_status_desc, 1, 30) as varchar(30))
        as lot_status_desc,
    cast(
        substring(source_business_unit_code, 1, 255) as text(
        255
        )
    ) as source_business_unit_code,
    cast(substring(business_unit_address_guid, 1, 255) as text(255))
        as business_unit_address_guid,
    cast(transaction_amt as number(27, 9)) as transaction_amt,
    cast(substring(reason_code, 1, 255) as varchar(255)) as reason_code,
    cast(transaction_date as timestamp_ntz(9)) as transaction_date,
    cast(substring(remark_txt, 1, 255) as varchar(255)) as remark_txt,
    cast(transaction_qty as number(27, 9)) as transaction_qty,
    cast(substring(transaction_uom, 1, 20) as varchar(20))
        as transaction_uom,
    cast(transaction_unit_cost as number(27, 9)) as transaction_unit_cost,
    cast(substring(transaction_currency, 1, 255) as varchar(20))
        as transaction_currency,
    cast(transaction_pri_uom_qty as number(27, 9))
        as transaction_pri_uom_qty,
    cast(transaction_pri_uom_unit_cost as number(27, 9))
         as transaction_pri_uom_unit_cost,
    cast(transaction_pri_uom_amt as number(27, 9))
         as transaction_pri_uom_amt,
    cast(substring(base_currency, 1, 20) as varchar(20)) as base_currency,
    cast(base_amt as number(27, 9)) as base_amt,
    cast(substring(source_system, 1, 255) as varchar(255))
        as source_system,
    cast(substring(source_document_type, 1, 20) as varchar(20))
        as source_document_type,
    cast(substring(source_original_document_type, 1, 20) as varchar(20))
        as source_original_document_type,
    cast(substring(reason_code_desc, 1, 255) as varchar(255))
        as reason_code_desc,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(transaction_kg_qty as number(27, 9)) as transaction_kg_qty,
    cast(transaction_lb_qty as number(27, 9)) as transaction_lb_qty,
    cast(substring(source_pallet_id, 1, 255) as varchar(255))
        as source_pallet_id,
    cast(substring(variant, 1, 255) as varchar(255)) as variant,
    cast(pallet_count as number(15, 2)) as pallet_count,
    cast(source_legacy as varchar(255)) as source_legacy,
    cast(substring(unique_key,1,255) as text(255) ) as unique_key
    from final
