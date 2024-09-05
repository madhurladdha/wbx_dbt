{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["sales", "gl", "std_je", "sales_gl_allocations"],
        unique_key='GL_DATE',
        incremental_strategy='delete+insert',
        full_refresh=false,
        pre_hook="""
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        delete from  {{ this }} where to_date(gl_date) >= (select dateadd(month, -2, date_trunc('month', current_date)))
        and source_legacy <> 'AX'
        {% endif %}     
        """
    )
}}

/*
GCAM Scenario:
    Allocation with validation Product Class
    Allocation without valid Product Class
    Allocation using Item from Work Order


Load strategy is snapshot model. We are just taking last 3 months of data and processing it incrementally .
In first load it will load complete history. It will load historical data from AX based on the column source_legacy.
If column source_legacy has value AX in it, it means history data is loaded and in next execution this model will run
incrementally. If source_legacy has no AX value in it, it will load historical AX data from conversion model.

Since, Account category is not present in AX world. Hence, based on source_object_id derived account_category.
select distinct SOURCE_OBJECT_ID,ACCOUNT_DESCRIPTION,account_category from wbx_prod_D365.dim.dim_wbx_account where SOURCE_OBJECT_ID in ('500010','500020','500030','500040');

*/

{% set trade_type = 'OP Admin Branded' %}
{% set account_category_list = "('PCOS STD - BOUGHT IN','PCOS STD - OTHER','PCOS STD - FG','PCOS STD - INTERCO')" %}

with
gl_trans as (
    select * from {{ ref("fct_wbx_fin_gl_trans") }}
    where
        upper(trim(account_category)) in {{ account_category_list }} and
        upper(document_company) in {{ env_var('DBT_D365_COMPANY_FILTER') }}
    {% if check_table_exists( this.schema, this.table ) == 'True' %} 
        and to_date(gl_date) >= (select dateadd(month, -2, date_trunc('month', current_date)))
        {% endif %}
),
dim_date_cte as (select * from {{ ref("src_dim_date") }}),
ax_hist as 
   (
    select * from {{ ref('conv_fct_wbx_sls_gl_pcos_std_je') }} 
        where GL_date>=(select dateadd(year,-1,FISCAL_YEAR_BEGIN_DT) from dim_date_cte where to_date(CALENDAR_DATE)=to_date(sysdate()))
   ),

item_ext as (select * from {{ ref("dim_wbx_item_ext") }}),



wo as (select * from {{ ref("mfg_wtx_wo_fact") }}),

allocbasis as (
    select * from {{ ref("int_f_wbx_sls_order_allocbasis_tradetype") }}
),

allocbasis_product_class as (
    select
        gl_month,
        company_code,
        item_guid,
        product_class_code,
        source_item_identifier,
        sum(perc_product_class) as perc_product_class
    from allocbasis group by all
),        --For when we have ONLY product class

allocbasis_no_sku_no_productclass as (
    select distinct
        gl_month,
        company_code,
        item_guid,
        source_item_identifier,
        product_class_code,
        sum(perc_month) as perc_month
    from allocbasis
    group by all
),

prod_class as (select distinct product_class_code from item_ext),

gl_logic as (
    select gt.*
    from gl_trans as gt
),

itm_ext_group as (
    select distinct
        item_guid,
        source_item_identifier,
        product_class_code
    from item_ext
),

/* No Allocation needed. If there is a sku and no product class, default the trade type to Dummy Trade Type*/
    /* No allocation needed,when valid sku and product class*/
scenario1 as (
    select
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        last_day(gl.gl_date, 'month') as gl_date, --for PCOS we are getting last date of each date.diff. from trade and gcam. Confirm from Melisa and Mason
        case
            when gl.source_document_type = 'GJ0' then 'Y' else 'N'
        end as journal_entry_flag,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no as fiscal_year_period_no,
        sum(gl.oc_txn_ledger_amt) as txn_ledger_amt,
        sum(gl.oc_base_ledger_amt) as base_ledger_amt,
        sum(gl.oc_phi_ledger_amt) as phi_ledger_amt,
        sum(gl.oc_pcomp_ledger_amt) as pcomp_ledger_amt,
        '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
        -- Field should reference Financial Tags
        itm_ext.product_class_code as product_class,
        -- Field should reference Financial Tags
        nvl(wof.source_item_identifier, itm_ext.source_item_identifier)
            as source_item_identifier,
        to_char(itm_ext.item_guid) as item_guid,
        trunc(current_date, 'DD') as load_date,
        trunc(current_date, 'DD') as update_date,
        'PCOS Option 1: Valid SKU and no product class' as union_logic

    from gl_logic as gl
    inner join dim_date_cte as dt on dt.calendar_date = gl.gl_date
    inner join wo as wof on wof.voucher = gl.reference2_txt
    --gl.sku=itm_ext.source_item_identifier, valid sku
    inner join
        itm_ext_group as itm_ext
        on wof.source_item_identifier = itm_ext.source_item_identifier
    where
        --PB:this condition not needed. since its inner join
        nvl(wof.source_item_identifier, itm_ext.source_item_identifier)
        is not null

    group by
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        gl.gl_date,
        case when gl.source_document_type = 'GJ0' then 'Y' else 'N' end,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no,
        itm_ext.product_class_code,
        nvl(wof.source_item_identifier, itm_ext.source_item_identifier),
        to_char(itm_ext.item_guid)
),

/* scenario2:There is a product class but no sku. Allocating to all sku's in product class that have volume. Default Trade Type to OP Admin Branded.*/
scenario2 as (
    select
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        last_day(gl.gl_date, 'month') as gl_date,
        case
            when gl.source_document_type = 'GJ0' then 'Y' else 'N'
        end as journal_entry_flag,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no as fiscal_year_period_no,
        sum(gl.oc_txn_ledger_amt * allobasic.perc_product_class)
            as txn_ledger_amt,
        sum(
            gl.oc_base_ledger_amt * allobasic.perc_product_class
        ) as base_ledger_amt,
        sum(gl.oc_phi_ledger_amt * allobasic.perc_product_class)
            as phi_ledger_amt,
        sum(
            gl.oc_pcomp_ledger_amt * allobasic.perc_product_class
        ) as pcomp_ledger_amt,
        '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
        -- Field should reference Financial Tags
        allobasic.product_class_code as product_class,
        -- Field should reference Financial Tags
        allobasic.source_item_identifier as source_item_identifier,
        allobasic.item_guid,
        trunc(current_date, 'DD') as load_date,
        trunc(current_date, 'DD') as update_date,
        'PCOS Option 2: Valid product class not SKU' as union_logic
    from gl_logic as gl
    inner join dim_date_cte as dt on dt.calendar_date = gl.gl_date
    left join
        itm_ext_group as itm_ext
        on gl.sku = itm_ext.source_item_identifier
    inner join prod_class on gl.product_class = prod_class.product_class_code
    left join wo as wof on wof.voucher = gl.reference2_txt--Just join on item
    inner join
        allocbasis_product_class as allobasic  --valid trade type
        on
            gl.product_class = allobasic.product_class_code
            and date_trunc('month', gl.gl_date) = allobasic.gl_month
            and gl.document_company = allobasic.company_code

    where
        (
            oc_txn_ledger_amt
            + oc_base_ledger_amt
            + oc_phi_ledger_amt
            + oc_pcomp_ledger_amt
        )
        <> 0
        and itm_ext.item_guid is null
        and wof.source_item_identifier is null
        --this is not needed, since we are doing inner join above
        and prod_class.product_class_code is not null
    group by
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        gl.gl_date,
        case when gl.source_document_type = 'GJ0' then 'Y' else 'N' end,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no,
        allobasic.product_class_code,
        allobasic.source_item_identifier,
        allobasic.item_guid, gl.trade_type
),

/*Scenario3: There is no product class and no sku. Allocating to all sku's in all product classes that have volume. Default Trade Type to OP Admin Branded.*/
scenario3 as (
    select
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        last_day(gl.gl_date, 'month') as gl_date,
        case
            when gl.source_document_type = 'GJ0' then 'Y' else 'N'
        end as journal_entry_flag,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no as fiscal_year_period_no,
        sum(gl.oc_txn_ledger_amt * allobasic.perc_month) as txn_ledger_amt,
        sum(gl.oc_base_ledger_amt * allobasic.perc_month) as base_ledger_amt,
        sum(gl.oc_phi_ledger_amt * allobasic.perc_month) as phi_ledger_amt,
        sum(gl.oc_pcomp_ledger_amt * allobasic.perc_month) as pcomp_ledger_amt,
        '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
        -- Field should reference Financial Tags
        allobasic.product_class_code as product_class,
        -- Field should reference Financial Tags
        allobasic.source_item_identifier as source_item_identifier,
        allobasic.item_guid as item_guid,
        trunc(current_date, 'DD') as load_date,
        trunc(current_date, 'DD') as update_date,
        'PCOS Option 3: invalid SKU and product class' as union_logic

    from gl_logic as gl
    inner join dim_date_cte as dt on dt.calendar_date = gl.gl_date
    left join
        itm_ext_group as itm_ext
        on gl.sku = itm_ext.source_item_identifier
    left join prod_class on gl.product_class = prod_class.product_class_code
    left join wo as wof on wof.voucher = gl.reference2_txt
    inner join
        allocbasis_no_sku_no_productclass as allobasic
        on
            allobasic.gl_month = date_trunc('month', gl.gl_date)
            and gl.document_company = allobasic.company_code

    where
        (
            oc_txn_ledger_amt
            + oc_base_ledger_amt
            + oc_phi_ledger_amt
            + oc_pcomp_ledger_amt
        )
        <> 0
        and wof.source_item_identifier is null
        and prod_class.product_class_code is null
        and itm_ext.source_item_identifier is null

    group by
        gl.source_system,
        gl.target_account_identifier,
        gl.account_guid,
        gl.source_object_id,
        gl.account_category,
        gl.business_unit_address_guid,
        gl.gl_date,
        case when gl.source_document_type = 'GJ0' then 'Y' else 'N' end,
        gl.document_company,
        gl.source_account_identifier,
        gl.source_business_unit_code,
        gl.base_currency,
        gl.transaction_currency,
        gl.phi_currency,
        gl.pcomp_currency,
        dt.report_fiscal_year_period_no,
        allobasic.product_class_code,
        allobasic.source_item_identifier,
        allobasic.item_guid


),

final_union as (
    select * from scenario1
    union all
    select * from scenario2
    union all
    select * from scenario3
),

final_d365 as (
    select
        cast(substring(source_system, 1, 10) as text(10)) as source_system,
        cast(
            substring(target_account_identifier, 1, 255) as text(255)
        ) as target_account_identifier,
        cast(account_guid as text(255)) as account_guid,
        cast(substring(source_object_id, 1, 255) as text(255))
            as source_object_id,
        cast(substring(account_category, 1, 255) as text(255))
            as account_category,
        cast(business_unit_address_guid as text(255))
            as business_unit_address_guid,
        cast(gl_date as date) as gl_date,
        cast(substring(journal_entry_flag, 1, 255) as text(255))
            as journal_entry_flag,
        cast(substring(document_company, 1, 255) as text(255))
            as document_company,
        cast(
            substring(source_account_identifier, 1, 255) as text(255)
        ) as source_account_identifier,
        cast(
            substring(source_business_unit_code, 1, 255) as text(255)
        ) as source_business_unit_code,
        cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
        cast(substring(transaction_currency, 1, 10) as text(10))
            as transaction_currency,
        cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
        cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
        cast(
            substring(fiscal_year_period_no, 1, 255) as text(255)
        ) as fiscal_year_period_no,
        cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
        cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
        cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
        cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
        cast(substring(trade_type, 1, 10) as text(10)) as trade_type,
        cast(substring(product_class, 1, 10) as text(10)) as product_class,
        cast(
            substring(source_item_identifier, 1, 255) as text(255)
        ) as source_item_identifier,
        cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
        cast(load_date as date) as load_date,
        cast(update_date as date) as update_date,
        cast(substring(union_logic, 1, 255) as text(255)) as union_logic,
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
                    "JOURNAL_ENTRY_FLAG",
                    "TRADE_TYPE",
                ]
            )
        }} as text(255)
        ) as unique_key,
        'D365' as source_legacy
    from final_union
),

final as (
    select
        source_system,
        target_account_identifier,
        account_guid,
        source_object_id,
        business_unit_address_guid,
        gl_date,
        journal_entry_flag,
        document_company,
        source_account_identifier,
        source_business_unit_code,
        base_currency,
        transaction_currency,
        phi_currency,
        pcomp_currency,
        fiscal_year_period_no,
        txn_ledger_amt,
        base_ledger_amt,
        phi_ledger_amt,
        pcomp_ledger_amt,
        trade_type,
        product_class,
        source_item_identifier,
        item_guid,
        load_date,
        update_date,
        union_logic,
        unique_key,
        account_category,
        source_legacy
    from final_d365
    {% if check_ax_hist_exists( this.schema, this.table ) == 'False' %}
 union all
 select 
        source_system,
        target_account_identifier,
        account_guid,
        source_object_id,
        business_unit_address_guid,
        gl_date,
        journal_entry_flag,
        document_company,
        source_account_identifier,
        source_business_unit_code,
        base_currency,
        transaction_currency,
        phi_currency,
        pcomp_currency,
        fiscal_year_period_no,
        txn_ledger_amt,
        base_ledger_amt,
        phi_ledger_amt,
        pcomp_ledger_amt,
        trade_type,
        product_class,
        source_item_identifier,
        item_guid,
        current_date as load_date,
        current_date as update_date,
        union_logic,
        unique_key,
        --null as account_category, 
        case 
            when source_object_id = '500030' then 'PCOS STD - BOUGHT IN'
            when source_object_id = '500020' then 'PCOS STD - OTHER'
            when source_object_id = '500010' then 'PCOS STD - FG'
            when source_object_id = '500040' then 'PCOS STD - INTERCO' 
        end as account_category,
        source_legacy
    from ax_hist
{% endif %}
)

select * from final

/*
Since, Account category is not present in AX world. Hence, based on source_object_id derived account_category.
select distinct SOURCE_OBJECT_ID,ACCOUNT_DESCRIPTION,account_category from wbx_prod_D365.dim.dim_wbx_account where SOURCE_OBJECT_ID in ('500010','500020','500030','500040');
*/