{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        tags=["hist", "pl","pl_hist"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        pre_hook="""
    
            {% if check_table_exists( this.schema, this.table ) == 'True' %}
                truncate table {{ this }}
            {% endif %}  

            """,
    )
}}

/*
GL Trade History from the P&L History file data set for IBE.
Load strategy is a full destructive load so will truncate and completely load.  This model would not be run regularly once deployed and confirmed.
*/

/* Trade Fields Mapping: as of 14-Mar-2024 there are 16 such fields for trade.  This will be un-pivoted into the gl_trade_fact structure.

Growth_Incentives	350030
EDLP	350020
Early_Settlement_Discount	350040
RSA_Incentives	350010
Retro	400010
AVP_Discount	600010
Off_Invoice	400010
Promo_Fixed_Funding	600020
Fixed_Annual_Payments	600040
Direct_Shopper_Marketing	610030
Other_Direct_Payments	600040
Indirect_Shopper_Marketing	350030
Category	610020
Other_Indirect_Payments	610020
Field_Marketing	610010
Other_Trade	600040

*/

with
    pl_hist as 
    (
        select *  from {{ ref("fct_wbx_sls_pl_ibehist") }}
    ),
    account_dim as 
    (
        select * from {{ ref("dim_wbx_account") }}
    ),
    item_ext as (
        select
            to_char(item_guid) as item_guid, source_item_identifier, product_class_code
        from {{ ref("dim_wbx_item_ext") }}
        group by item_guid, source_item_identifier, product_class_code
    ),
    trade_type_set as (select distinct company_code, trade_type_code from {{ ref("dim_wbx_cust_planning") }} ),
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),

    /*Unpivot / Normalize the history data set */
    /* For each of the incoming fields, for PCOS Std there are 16 applicable, the code is un-pivoting from columns to rows to fit in with the GL Trade Fact structure.
        There are 4 unpivoted fields as there are 4 different currency codes carried for each of the applicable fields.
        The result of these 4 unpivots is to cause a large "explosion" of rows.  They will be collapsed down in subsequent CTE steps.
    */
    pl_hist_unpivot as
    (
        select *
        from pl_hist
        unpivot 
            (txn_ledger_amt for txn_pivot_field in (txn_growth_incentives,txn_edlp,txn_early_settlement_discount,txn_rsa_incentives,txn_retro,txn_avp_discount,txn_off_invoice,txn_promo_fixed_funding,txn_fixed_annual_payments,txn_direct_shopper_marketing,txn_other_direct_payments,txn_indirect_shopper_marketing,txn_category,txn_other_indirect_payments,txn_field_marketing,txn_other_trade))
        unpivot
            (pcomp_ledger_amt for pcomp_pivot_field in (pcomp_growth_incentives,pcomp_edlp,pcomp_early_settlement_discount,pcomp_rsa_incentives,pcomp_retro,pcomp_avp_discount,pcomp_off_invoice,pcomp_promo_fixed_funding,pcomp_fixed_annual_payments,pcomp_direct_shopper_marketing,pcomp_other_direct_payments,pcomp_indirect_shopper_marketing,pcomp_category,pcomp_other_indirect_payments,pcomp_field_marketing,pcomp_other_trade))
        unpivot
            (phi_ledger_amt for phi_pivot_field in (phi_growth_incentives,phi_edlp,phi_early_settlement_discount,phi_rsa_incentives,phi_retro,phi_avp_discount,phi_off_invoice,phi_promo_fixed_funding,phi_fixed_annual_payments,phi_direct_shopper_marketing,phi_other_direct_payments,phi_indirect_shopper_marketing,phi_category,phi_other_indirect_payments,phi_field_marketing,phi_other_trade))
        unpivot
            (base_ledger_amt for base_pivot_field in (base_growth_incentives,base_edlp,base_early_settlement_discount,base_rsa_incentives,base_retro,base_avp_discount,base_off_invoice,base_promo_fixed_funding,base_fixed_annual_payments,base_direct_shopper_marketing,base_other_direct_payments,base_indirect_shopper_marketing,base_category,base_other_indirect_payments,base_field_marketing,base_other_trade))
    ),

    /* This CTE statement is doing multiple transformations after the unpivots.
    1) Assigning the appropriate single GL Account for each incoming field.  Cannot be broken out further w/o significant redesign and dev.
    2) The unpivot statements above cause a large "explosion" of rows that are empty.  The filters are suppressing any resultant unpivot rows that
        either have all zeros in their ledger outputs OR are not the aligned amount fields across currencies.  Basically we only want rows 
        from the cartesian (cross join) that align across TXN, PCOMP, BASE, and PHI.
    */
    pl_hist_unpivot_acct as
    (
        select * ,
            case 
                when hist.txn_pivot_field='TXN_GROWTH_INCENTIVES' then '350030'
                when hist.txn_pivot_field='TXN_EDLP' then '350020'
                when hist.txn_pivot_field='TXN_EARLY_SETTLEMENT_DISCOUNT' then '350040'
                when hist.txn_pivot_field='TXN_RSA_INCENTIVES' then '350010'
                when hist.txn_pivot_field='TXN_RETRO' then '400010'
                when hist.txn_pivot_field='TXN_AVP_DISCOUNT' then '600010'
                when hist.txn_pivot_field='TXN_OFF_INVOICE' then '400010'
                when hist.txn_pivot_field='TXN_PROMO_FIXED_FUNDING' then '600020'
                when hist.txn_pivot_field='TXN_FIXED_ANNUAL_PAYMENTS' then '600040'
                when hist.txn_pivot_field='TXN_DIRECT_SHOPPER_MARKETING' then '610030'
                when hist.txn_pivot_field='TXN_OTHER_DIRECT_PAYMENTS' then '600040'
                when hist.txn_pivot_field='TXN_INDIRECT_SHOPPER_MARKETING' then '350030'
                when hist.txn_pivot_field='TXN_CATEGORY' then '610020'
                when hist.txn_pivot_field='TXN_OTHER_INDIRECT_PAYMENTS' then '610020'
                when hist.txn_pivot_field='TXN_FIELD_MARKETING' then '610010'
                when hist.txn_pivot_field='TXN_OTHER_TRADE' then '600040'
                else '999999' end as source_object_id
        from pl_hist_unpivot hist
        where txn_ledger_amt+pcomp_ledger_amt+phi_ledger_amt+base_ledger_amt<>0
        and upper(trim(substr(txn_pivot_field,5,255)))=upper(trim(substr(pcomp_pivot_field,7,255)))
        and upper(trim(substr(txn_pivot_field,5,255)))=upper(trim(substr(phi_pivot_field,5,255)))
        and upper(trim(substr(txn_pivot_field,5,255)))=upper(trim(substr(base_pivot_field,6,255)))
    ),

    
    /* No allocation.  This is straight from the historical data set*/
    pl_hist_trade as (
        select
            hist.source_system,
            account_dim.tagetik_account as target_account_identifier,
            account_dim.account_guid,
            hist.source_object_id,
            account_dim.account_category,
            null as business_unit_address_guid,  --not required so defaulting this
            hist.date as gl_date,
            'N' as journal_entry_flag,
            hist.company_code as document_company,
            account_dim.source_account_identifier,
            account_dim.source_business_unit_code,
            hist.base_currency,
            hist.transaction_currency,
            hist.phi_currency,
            hist.pcomp_currency,
            dt.report_fiscal_year_period_no as fiscal_year_period_no,
            sum(hist.txn_ledger_amt) as txn_ledger_amt,
            sum(hist.base_ledger_amt) as base_ledger_amt,
            sum(hist.phi_ledger_amt) as phi_ledger_amt,
            sum(hist.pcomp_ledger_amt) as pcomp_ledger_amt,
            hist.trade_type as trade_type,        -- Field should reference Financial Tags, picked from gl
            itm_ext.product_class_code as product_class,  -- Derived from valid SKU
            hist.sku as source_item_identifier,   -- Field should reference Financial Tags,picked from gl sku
            to_char(itm_ext.item_guid) as item_guid, --this can be commented out and derived directly from sku
            'History' as union_logic,   --default to 'History'.  not really needed.
            current_date as load_date,
            current_date as update_date
        from pl_hist_unpivot_acct hist
        inner join dim_date_cte dt on dt.calendar_date = hist.date             --Should be a valid GL Date
        /* Both inner joins to item_ext and trade_type_set to confirm valid values for both. */
        inner join item_ext itm_ext on hist.sku = itm_ext.source_item_identifier  --Joins with item.  Confirms valid sku.
        inner join trade_type_set       --Joins with trade types.  Confirm valid trade_type.
            on hist.trade_type = trade_type_set.trade_type_code
            and hist.company_code = trade_type_set.company_code
        inner join account_dim          --Join w/ the account dimension to get those attributes.
            on hist.source_object_id = account_dim.source_object_id
            and hist.company_code = account_dim.source_company_code
        where
             (
                txn_ledger_amt
                + base_ledger_amt
                + phi_ledger_amt
                + pcomp_ledger_amt
            ) <> 0 
        group by
            hist.source_system,
            account_dim.tagetik_account,
            account_dim.account_guid,
            hist.source_object_id,
            account_dim.account_category,
            --hist.business_unit_address_guid,
            hist.date,
            hist.company_code,
            account_dim.source_account_identifier,
            account_dim.source_business_unit_code,
            hist.base_currency,
            hist.transaction_currency,
            hist.phi_currency,
            hist.pcomp_currency,
            dt.report_fiscal_year_period_no,
            hist.trade_type,
            itm_ext.product_class_code,
            hist.sku,
            itm_ext.item_guid,
            hist.txn_pivot_field
    ),

    final as 
    (
        select * from pl_hist_trade
    )
        
    select
        cast(substring(source_system, 1, 10) as text(10)) as source_system ,
        cast(substring(target_account_identifier, 1, 255) as text(255)) as target_account_identifier,
        cast(substring(account_guid, 1, 255) as text(255)) as account_guid,
        cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
        cast(substring(account_category, 1, 255) as text(255)) as account_category,
        cast(substring(business_unit_address_guid, 1, 60) as text(60)) as business_unit_address_guid,
        cast(gl_date as date) as gl_date,
        cast(substring(document_company, 1, 255) as text(255)) as document_company,
        cast(substring(source_account_identifier, 1, 255) as text(255)) as source_account_identifier,
        cast(substring(source_business_unit_code, 1, 255) as text(255)) as source_business_unit_code,
        cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
        cast(substring(transaction_currency, 1, 10) as text(10)) as transaction_currency,
        cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
        cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
        cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
        cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
        cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
        cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
        cast(substring(source_item_identifier, 1, 255) as text(255)) as source_item_identifier,
        cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
        cast(substring(fiscal_year_period_no, 1, 255) as text(255)) as fiscal_year_period_no,
        cast(substring(trade_type, 1, 10) as text(10)) as trade_type,
        cast(substring(product_class, 1, 10) as text(10)) as product_class,
        cast(substring(journal_entry_flag, 1, 10) as text(10)) as journal_entry_flag,
        cast(substring(union_logic, 1, 255) as text(255)) as union_logic,
        cast(load_date as date) as load_date,
        cast(update_date as date) as update_date,
        cast(
            {{
                dbt_utils.surrogate_key(
                    [
                        "SOURCE_ITEM_IDENTIFIER",
                        "SOURCE_BUSINESS_UNIT_CODE",
                        "TARGET_ACCOUNT_IDENTIFIER",
                        "TRANSACTION_CURRENCY",
                        "DOCUMENT_COMPANY",
                        "UNION_LOGIC",
                        "GL_DATE",
                        "SOURCE_OBJECT_ID",
                        "TRADE_TYPE",
                        "JOURNAL_ENTRY_FLAG",
                    ]
                )
            }} as text(255)
        ) as unique_key 
from final
