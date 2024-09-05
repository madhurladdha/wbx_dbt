{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_fact","ax_hist_inventory"]
    )
}}
/*source_business_unit_code is changing so need to re genertae lot_guid and location guid*/
with
old_fct as (
    select * 
    from {{ source("WBX_PROD_FACT", "fct_wbx_inv_trans_ledger") }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
), --make sure this flag is set to yes, this allows the system to pull the history. Without this, history will not come through.

old_plant as (
    select
        source_business_unit_code_new,
        source_business_unit_code,
        plantdc_address_guid_new,
        plantdc_address_guid
    from {{ ref('conv_dim_wbx_plant_dc') }}
),

converted_fct as (
    select
        cast(substring(a.related_address_number, 1, 255) as varchar(255))
            as related_address_number,
        cast(substring(a.address_guid, 1, 255) as varchar(255)) as address_guid,
        cast(substring(a.document_type, 1, 20) as varchar(20)) as document_type,
        cast(substring(a.original_document_type, 1, 20) as varchar(20))
            as original_document_type,
        cast(a.gl_date as timestamp_ntz(9)) as gl_date,
        cast(substring(a.document_number, 1, 255) as varchar(255))
            as document_number,
        cast(substring(a.original_document_number, 1, 255) as varchar(255))
            as original_document_number,
        cast(substring(a.source_item_identifier, 1, 255) as varchar(255))
            as source_item_identifier,
        cast(substring(a.item_guid, 1, 255) as varchar(255)) as item_guid,
        cast(substring(a.document_company, 1, 20) as varchar(20))
            as document_company,
        cast(substring(a.original_document_company, 1, 20) as varchar(20))
            as original_document_company,
        cast(a.original_line_number as number(38, 10)) as original_line_number,
        cast(a.line_number as number(38, 10)) as line_number,
        cast(substring(a.source_location_code, 1, 255) as varchar(255))
            as source_location_code,
        cast(substring(a.location_guid, 1, 255) as varchar(255)) as location_guid_old,
        cast(substring({{ dbt_utils.surrogate_key
                               (
                                    [
                                        "a.SOURCE_SYSTEM",
                                        "upper(a.SOURCE_LOCATION_CODE)",
                                        "upper(trim(plnt.source_business_unit_code_new))",
                                    ]
                                )
                        }}, 1, 255) as varchar(255)) as location_guid,


        cast(substring(a.source_lot_code, 1, 255) as varchar(255))
            as source_lot_code,
        cast(substring(a.lot_guid, 1, 255) as varchar(255)) as lot_guid_old,
        cast(substring({{
                                dbt_utils.surrogate_key(
                                    [
                                        "a.SOURCE_SYSTEM",
                                        "upper(trim(plnt.source_business_unit_code_new))",
                                        "a.SOURCE_ITEM_IDENTIFIER",
                                        "upper(a.source_lot_code)",
                                    ]
                                )
                        }}, 1, 255) as varchar(255)) as lot_guid,
        cast(substring(a.lot_status_code, 1, 255) as varchar(255))
            as lot_status_code,
        cast(substring(a.lot_status_desc, 1, 30) as varchar(30))
            as lot_status_desc,
        cast(
            substring(plnt.source_business_unit_code_new, 1, 255) as text(
                255
            )
        ) as source_business_unit_code,
        cast(
            substring(plnt.source_business_unit_code, 1, 255) as text(255)
        ) as source_business_unit_code_old,
        cast(substring(plnt.plantdc_address_guid_new, 1, 255) as text(255))
            as business_unit_address_guid,
        cast(substring(plnt.plantdc_address_guid, 1, 255) as text(255))
            as business_unit_address_guid_old,
        cast(a.transaction_amt as number(27, 9)) as transaction_amt,
        cast(substring(a.reason_code, 1, 255) as varchar(255)) as reason_code,
        cast(a.transaction_date as timestamp_ntz(9)) as transaction_date,
        cast(substring(a.remark_txt, 1, 255) as varchar(255)) as remark_txt,
        cast(a.transaction_qty as number(27, 9)) as transaction_qty,
        cast(substring(a.transaction_uom, 1, 20) as varchar(20))
            as transaction_uom,
        cast(a.transaction_unit_cost as number(27, 9)) as transaction_unit_cost,
        cast(substring(a.transaction_currency, 1, 255) as varchar(20))
            as transaction_currency,
        cast(a.transaction_pri_uom_qty as number(27, 9))
            as transaction_pri_uom_qty,
        cast(a.transaction_pri_uom_unit_cost as number(27, 9))
            as transaction_pri_uom_unit_cost,
        cast(a.transaction_pri_uom_amt as number(27, 9))
            as transaction_pri_uom_amt,
        cast(substring(a.base_currency, 1, 20) as varchar(20)) as base_currency,
        cast(a.base_amt as number(27, 9)) as base_amt,
        cast(substring(a.source_system, 1, 255) as varchar(255))
            as source_system,
        cast(substring(a.source_document_type, 1, 20) as varchar(20))
            as source_document_type,
        cast(substring(a.source_original_document_type, 1, 20) as varchar(20))
            as source_original_document_type,
        cast(substring(a.reason_code_desc, 1, 255) as varchar(255))
            as reason_code_desc,
        cast(a.load_date as timestamp_ntz(9)) as load_date,
        cast(a.update_date as timestamp_ntz(9)) as update_date,
        cast(a.transaction_kg_qty as number(27, 9)) as transaction_kg_qty,
        cast(a.transaction_lb_qty as number(27, 9)) as transaction_lb_qty,
        cast(substring(a.source_pallet_id, 1, 255) as varchar(255))
            as source_pallet_id,
        cast(substring(a.variant, 1, 255) as varchar(255)) as variant,
        cast(a.pallet_count as number(15, 2)) as pallet_count,
        cast(substring(a.unique_key, 1, 255) as varchar(255)) as unique_key
    from old_fct as a
    left join
        old_plant as plnt
        on a.business_unit_address_guid = plnt.plantdc_address_guid
)

-- where DATEDIFF(day, to_date(TRANSACTION_DATE), current_date) <=3

select * from converted_fct

