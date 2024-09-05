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

/* GCAM Fields Mapping: as of 14-Mar-2024 there are 21 such fields.  This will be un-pivoted into the gl_trade_fact structure.  Code is simply using the first
    such account that falls under the PCOS Std Account Category.  Can only specify one.

Marketing_Agency_Fees	620010
Research	620020
Continuous_Research	620030
Market_Research	620040
Sponsorship	620050
Sales_Promotions	620060
Pack_Artwork_Design	620070
POS_Materials	620080
Samples_Issued	620090
PR	620100
Advertising_TV	630010
TV_Advertising_Production	630020
Press_Advertising_Consumer	630030
Press_Advertising_Production_Consumer	630040
Radio_Time	630050
Radio_Time_Production	630060
Website_Marketing	630070
Poster_Space	630080
Poster_Production	630090
Digital_Media	630100
Digital_Media_Production	630110


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
    /* For each of the incoming fields, for Trade there are 16 applicable, the code is un-pivoting from columns to rows to fit in with the GL Trade Fact structure.
        There are 4 unpivoted fields as there are 4 different currency codes carried for each of the applicable fields.
        The result of these 4 unpivots is to cause a large "explosion" of rows.  They will be collapsed down in subsequent CTE steps.
    */
    pl_hist_unpivot as
    (
        select *
        from pl_hist
        unpivot 
            (txn_ledger_amt for txn_pivot_field in (txn_marketing_agency_fees,txn_research,txn_continuous_research,txn_market_research,txn_sponsorship,txn_sales_promotions,txn_pack_artwork_design,txn_pos_materials,txn_samples_issued,txn_pr,txn_advertising_tv,txn_tv_advertising_production,txn_press_advertising_consumer,txn_press_advertising_production_consumer,txn_radio_time,txn_radio_time_production,txn_website_marketing,txn_poster_space,txn_poster_production,txn_digital_media,txn_digital_media_production))
        unpivot
            (pcomp_ledger_amt for pcomp_pivot_field in (pcomp_marketing_agency_fees,pcomp_research,pcomp_continuous_research,pcomp_market_research,pcomp_sponsorship,pcomp_sales_promotions,pcomp_pack_artwork_design,pcomp_pos_materials,pcomp_samples_issued,pcomp_pr,pcomp_advertising_tv,pcomp_tv_advertising_production,pcomp_press_advertising_consumer,pcomp_press_advertising_production_consumer,pcomp_radio_time,pcomp_radio_time_production,pcomp_website_marketing,pcomp_poster_space,pcomp_poster_production,pcomp_digital_media,pcomp_digital_media_production))
        unpivot
            (phi_ledger_amt for phi_pivot_field in (phi_marketing_agency_fees,phi_research,phi_continuous_research,phi_market_research,phi_sponsorship,phi_sales_promotions,phi_pack_artwork_design,phi_pos_materials,phi_samples_issued,phi_pr,phi_advertising_tv,phi_tv_advertising_production,phi_press_advertising_consumer,phi_press_advertising_production_consumer,phi_radio_time,phi_radio_time_production,phi_website_marketing,phi_poster_space,phi_poster_production,phi_digital_media,phi_digital_media_production))
        unpivot
            (base_ledger_amt for base_pivot_field in (base_marketing_agency_fees,base_research,base_continuous_research,base_market_research,base_sponsorship,base_sales_promotions,base_pack_artwork_design,base_pos_materials,base_samples_issued,base_pr,base_advertising_tv,base_tv_advertising_production,base_press_advertising_consumer,base_press_advertising_production_consumer,base_radio_time,base_radio_time_production,base_website_marketing,base_poster_space,base_poster_production,base_digital_media,base_digital_media_production))
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
                when hist.txn_pivot_field='TXN_MARKETING_AGENCY_FEES' then '620010'
                when hist.txn_pivot_field='TXN_RESEARCH' then '620020'
                when hist.txn_pivot_field='TXN_CONTINUOUS_RESEARCH' then '620030'
                when hist.txn_pivot_field='TXN_MARKET_RESEARCH' then '620040'
                when hist.txn_pivot_field='TXN_SPONSORSHIP' then '620050'
                when hist.txn_pivot_field='TXN_SALES_PROMOTIONS' then '620060'
                when hist.txn_pivot_field='TXN_PACK_ARTWORK_DESIGN' then '620070'
                when hist.txn_pivot_field='TXN_POS_MATERIALS' then '620080'
                when hist.txn_pivot_field='TXN_SAMPLES_ISSUED' then '620090'
                when hist.txn_pivot_field='TXN_PR' then '620100'
                when hist.txn_pivot_field='TXN_ADVERTISING_TV' then '630010'
                when hist.txn_pivot_field='TXN_TV_ADVERTISING_PRODUCTION' then '630020'
                when hist.txn_pivot_field='TXN_PRESS_ADVERTISING_CONSUMER' then '630030'
                when hist.txn_pivot_field='TXN_PRESS_ADVERTISING_PRODUCTION_CONSUMER' then '630040'
                when hist.txn_pivot_field='TXN_RADIO_TIME' then '630050'
                when hist.txn_pivot_field='TXN_RADIO_TIME_PRODUCTION' then '630060'
                when hist.txn_pivot_field='TXN_WEBSITE_MARKETING' then '630070'
                when hist.txn_pivot_field='TXN_POSTER_SPACE' then '630080'
                when hist.txn_pivot_field='TXN_POSTER_PRODUCTION' then '630090'
                when hist.txn_pivot_field='TXN_DIGITAL_MEDIA' then '630100'
                when hist.txn_pivot_field='TXN_DIGITAL_MEDIA_PRODUCTION' then '630110'
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
