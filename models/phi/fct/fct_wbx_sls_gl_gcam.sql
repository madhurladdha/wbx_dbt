{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
        tags=["sales", "gl", "gcam", "sales_gl_gcam"],
        unique_key='GL_DATE',
        incremental_strategy='delete+insert',
        full_refresh=false,
        pre_hook="""
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        delete from  {{ this }} where to_date(gl_date) >= (select dateadd(month, -2, date_trunc('month', current_date)))
        {% endif %}  
                
        """
    )
}}
/*
GCAM Scenario:
    There is a product class and no sku.
    No product class and no sku.
    There is a product class and a sku.

Load strategy is snapshot model. We are just taking last 3 months of data and processing it incrementally . 
In first load it will load complete history

Pending part: Account list needs to be updated based on the response from Dave on Mike's email
Trade_type needs to be confirmed
Confirm Unique key for gcam
*/

--add casting and date part

{% set trade_type = 'OP Admin Branded' %} -- default trade_type also needs to be confirmed.
{% set account_category_list = "('CONSUMER MEDIA','CONSUMER RESEARCH')" %}

with
    gl as (
        select * from {{ ref("fct_wbx_fin_gl_trans") }} 
        where upper(trim(account_category)) in {{account_category_list}} and 
        upper(document_company) in {{env_var('DBT_D365_COMPANY_FILTER')}} 
        {% if check_table_exists( this.schema, this.table ) == 'True' %} 
        and to_date(gl_date) >= (select dateadd(month, -2, date_trunc('month', current_date)))
        {% endif %}
    ),
    item_ext as ( select distinct item_guid, source_item_identifier, product_class_code from {{ ref("dim_wbx_item_ext") }}
    ),
    prod_class as (
        select distinct product_class_code
        from {{ ref("dim_wbx_item_ext") }}
    ),
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    allo_basic as (select * from {{ ref("int_f_wbx_sls_order_allocbasis_tradetype") }}),
    allo_basic_prod_class as (select gl_month,company_code,source_item_identifier,item_guid,product_class_code,sum(perc_product_class) perc_product_class from allo_basic group by all),
    allo_basic_prod_class_sku as (select gl_month,company_code,source_item_identifier,item_guid,product_class_code,sum(perc_month) perc_month from allo_basic group by all),
/* scenario1 :There is a product class but no sku. Allocating to all sku's in product class that have volume. Default Trade Type to OP Admin Branded.*/
scenario1 as 
(
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
    dt.report_fiscal_year_period_no,
    sum(gl.oc_txn_ledger_amt * allobasic.perc_product_class) as txn_ledger_amt,
    sum(gl.oc_base_ledger_amt * allobasic.perc_product_class) as base_ledger_amt,
    sum(gl.oc_phi_ledger_amt * allobasic.perc_product_class) as phi_ledger_amt,
    sum(gl.oc_pcomp_ledger_amt * allobasic.perc_product_class) as pcomp_ledger_amt,
    '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
    gl.product_class as product_class,  -- Field should reference Financial Tags
    allobasic.source_item_identifier as source_item_identifier,  -- Field should reference Financial Tags
    to_char(allobasic.item_guid) as item_guid,
    'GCAM Option 1:Valid product class not SKU' as union_logic,
    current_date as load_date,
    current_date as update_date
from gl
inner join dim_date_cte dt on dt.calendar_date = gl.gl_date
inner join prod_class on gl.product_class = prod_class.product_class_code 
left join item_ext itm_ext on gl.sku = itm_ext.source_item_identifier --invalid sku condition
inner join
    allo_basic_prod_class allobasic
    on allobasic.gl_month = date_trunc('month', gl.gl_date)
    and allobasic.product_class_code = gl.product_class 
    and allobasic.company_code = gl.document_company
    /*Assumption is product_class is same as product_class_code in allobasic table*/
where
     itm_ext.source_item_identifier is null
    and (
        oc_txn_ledger_amt + oc_base_ledger_amt + oc_phi_ledger_amt + oc_pcomp_ledger_amt
    )
    <> 0
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
    gl.product_class,  -- Field should reference Financial Tags
    allobasic.source_item_identifier,
    allobasic.item_guid
),
/* scenario2:There is no product class and no sku. Allocating to all sku's in all product classes that have volume. Default Trade Type to OP Admin Branded.*/
scenario2 as 
( 
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
    dt.report_fiscal_year_period_no,
    sum(gl.oc_txn_ledger_amt * allobasic.perc_month) as txn_ledger_amt,
    sum(gl.oc_base_ledger_amt * allobasic.perc_month) as base_ledger_amt,
    sum(gl.oc_phi_ledger_amt * allobasic.perc_month) as phi_ledger_amt,
    sum(gl.oc_pcomp_ledger_amt * allobasic.perc_month) as pcomp_ledger_amt,
    '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
    allobasic.product_class_code as product_class,  -- Field should reference Financial Tags
    allobasic.source_item_identifier as source_item_identifier,  -- Field should reference Financial Tags
    to_char(allobasic.item_guid) as item_guid,
    'GCAM Option 2: Invalid product class and sku' as union_logic,
    current_date as load_date,
    current_date as update_date
from gl
inner join dim_date_cte dt on dt.calendar_date = gl.gl_date
left join item_ext itm_ext on gl.sku = itm_ext.source_item_identifier and gl.product_class = itm_ext.product_class_code --no produt and sku
inner join allo_basic_prod_class_sku allobasic 
on allobasic.gl_month = date_trunc('month', gl.gl_date)
and allobasic.company_code = gl.document_company
where
    itm_ext.source_item_identifier is null and 
    itm_ext.product_class_code is null and
    (oc_txn_ledger_amt + oc_base_ledger_amt + oc_phi_ledger_amt + oc_pcomp_ledger_amt)<> 0
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
/* scenario3:No Allocation needed. If there is a sku and  product class, default the trade type to Dummy Trade Type*/
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
    dt.report_fiscal_year_period_no,
    sum(gl.oc_txn_ledger_amt) as txn_ledger_amt,
    sum(gl.oc_base_ledger_amt) as base_ledger_amt,
    sum(gl.oc_phi_ledger_amt) as phi_ledger_amt,
    sum(gl.oc_pcomp_ledger_amt) as pcomp_ledger_amt,
    '{{ trade_type }}' as trade_type,  -- Assigning Dummy Trade Type
    gl.product_class as product_class,  -- Field should reference Financial Tags
    gl.sku as source_item_identifier,  -- Field should reference Financial Tags
    itm_ext.item_guid as item_guid,
    'GCAM Option 3:Valid product class and sku' as union_logic,
    current_date as load_date,
    current_date as update_date
from gl
inner join dim_date_cte dt on dt.calendar_date = gl.gl_date
inner join item_ext itm_ext on gl.sku = itm_ext.source_item_identifier and gl.product_class = itm_ext.product_class_code --valid sku and product class
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
    gl.product_class,
    gl.sku,
    itm_ext.item_guid
),
final as 
(
    select * from scenario1
    union all
    select * from scenario2
    union all
    select * from scenario3
)
select
    cast(substring(source_system, 1, 10) as text(10)) as source_system ,
    cast(substring(target_account_identifier, 1, 255) as text(255) ) as target_account_identifier,
    cast(substring(account_guid, 1, 255) as text(255)) as account_guid,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(account_category, 1, 255) as text(255)) as account_category,
    cast( substring(business_unit_address_guid, 1, 60) as text(60) ) as business_unit_address_guid,
    cast(gl_date as date) as gl_date,
    cast(substring(document_company, 1, 255) as text(255)) as document_company,
    cast( substring(source_account_identifier, 1, 255) as text(255) ) as source_account_identifier,
    cast( substring(source_business_unit_code, 1, 255) as text(255) ) as source_business_unit_code,
    cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
    cast(substring(transaction_currency, 1, 10) as text(10)) as transaction_currency,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
    cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
    cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
    cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
    cast( substring(source_item_identifier, 1, 255) as text(255) ) as source_item_identifier,
    cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
    cast( substring(report_fiscal_year_period_no, 1, 255) as text(255) ) as fiscal_year_period_no,
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