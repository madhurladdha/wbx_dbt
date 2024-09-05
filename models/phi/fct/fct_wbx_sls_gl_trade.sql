{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        on_schema_change="sync_all_columns",
        tags=["sales", "gl", "sales_gl_allocations"],
        snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
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

GL Trade Scenario:
    There is a SKU and Trade Type
    There is a SKU but no Trade Type
    There is no SKU but there is a Trade Type
    There is no SKU and no Trade Type

Load strategy is snapshot model. We are just taking last 3 months of data and processing it incrementally . 
In first load it will load complete history. It will load historical data from AX based on the column source_legacy.
If column source_legacy has value AX in it, it means history data is loaded and in next execution this model will run 
incrementally. If source_legacy has no AX value in it, it will load historical AX data from conversion model.

*/
{% set account_category_list = "('INDIRECT FIXED TRADE','DIRECT FIXED TRADE','SALESDISCOUNTS','VARIABLETRADE')" %}

with
    gl_trans as 
    (
        select 
            * 
        from {{ ref("fct_wbx_fin_gl_trans") }} 
        where upper(trim(account_category)) in {{account_category_list}} and 
        upper(document_company) in {{env_var('DBT_D365_COMPANY_FILTER')}} 
        {% if check_table_exists( this.schema, this.table ) == 'True' %} 
        and to_date(gl_date) >= (select dateadd(month, -2, date_trunc('month', current_date)))
        {% endif %}
    ),
    item_ext as (
        select
            to_char(item_guid) as item_guid, source_item_identifier, product_class_code
        from {{ ref("dim_wbx_item_ext") }}
        group by item_guid, source_item_identifier, product_class_code
    ),
    dim_date_cte as (select * from {{ ref("src_dim_date") }}),
    ax_hist as 
        (
        select * from {{ ref('conv_fct_wbx_sls_gl_trade') }} 
         where GL_date>=(select dateadd(year,-1,FISCAL_YEAR_BEGIN_DT) from dim_date_cte where to_date(CALENDAR_DATE)=to_date(sysdate()))
         ),
    trade_type_set as (select distinct company_code, trade_type_code from {{ ref("dim_wbx_cust_planning") }} ),
    
    allocbasis as (select * from {{ ref("int_f_wbx_sls_order_allocbasis_tradetype") }}),
    allocbasis_sku as (select gl_month,company_code, source_item_identifier, item_guid,trade_type_code, 
    sum(perc_item) perc_item  from allocbasis group by all),   --For when we have ONLY SKU
    allocbasis_trade_type as (select gl_month,company_code,item_guid,product_class_code,source_item_identifier,trade_type_code, sum(perc_trade_type) perc_trade_type 
    from allocbasis group by all),        --For when we have ONLY Trade Type. 
    allocbasis_no_sku_no_tradetype as (select distinct gl_month,company_code,trade_type_code,item_guid, source_item_identifier,product_class_code,
    sum(perc_month) perc_month from allocbasis group by all), -- NO SKU and Trade Type
    /* Scenario 1: Valid SKU and Valid Trade Type in the GL.  No allocation Required*/
    scenario1 as (
        select
            gl.source_system,
            gl.target_account_identifier,
            gl.account_guid,
            gl.source_object_id,
            gl.account_category,
            gl.business_unit_address_guid,
            gl.gl_date,
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
            gl.trade_type as trade_type,        -- Field should reference Financial Tags, picked from gl
            itm_ext.product_class_code as product_class,  -- Derived from valid SKU
            gl.sku as source_item_identifier,   -- Field should reference Financial Tags,picked from gl sku
            to_char(itm_ext.item_guid) as item_guid, --this can be commented out and derived directly from sku
            'Trade Option 1: Valid SKU and Trade Type' as union_logic,
            current_date as load_date,
            current_date as update_date
        from gl_trans gl
        inner join dim_date_cte dt on dt.calendar_date = gl.gl_date             --Should be a valid GL Date
        /* Both inner joins to item_ext and trade_type_set to confirm valid values for both. */
        inner join item_ext itm_ext on gl.sku = itm_ext.source_item_identifier  --Joins when a valid SKU, do we need to consider company_code ??
        inner join trade_type_set 
            on gl.trade_type = trade_type_set.trade_type_code
            and gl.document_company = trade_type_set.company_code  --Joins when a valid Trade Type
        where
             (
                oc_txn_ledger_amt
                + oc_base_ledger_amt
                + oc_phi_ledger_amt
                + oc_pcomp_ledger_amt
            ) <> 0 
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
            gl.trade_type,
            itm_ext.product_class_code,
            gl.sku,
            itm_ext.item_guid
    ),
--select * from scenario1
/* Scenario 2: Valid SKU and NO Valid Trade Type in the GL.  Allocate to applicable Trade Types.*/
    scenario2 as (
        select
            gl.source_system,
            gl.target_account_identifier,
            gl.account_guid,
            gl.source_object_id,
            gl.account_category,
            gl.business_unit_address_guid,
            gl.gl_date,
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
            sum(gl.oc_txn_ledger_amt * allobasic.perc_item) as txn_ledger_amt, 
            --this amount must be equal to all the records from GL for this scenario.
            sum(gl.oc_base_ledger_amt * allobasic.perc_item) as base_ledger_amt,
            sum(gl.oc_phi_ledger_amt * allobasic.perc_item) as phi_ledger_amt,
            sum(gl.oc_pcomp_ledger_amt * allobasic.perc_item) as pcomp_ledger_amt,
            allobasic.trade_type_code as trade_type,            -- In this scenario, FinTag would be invalid.
            itm_ext.product_class_code as product_class,  -- Derived from valid SKU
            gl.sku as source_item_identifier,               -- Field should reference Financial Tags
            to_char(allobasic.item_guid) as item_guid, 
            'Trade Option 2: Valid SKU not Trade Type' as union_logic,
            current_date as load_date,
            current_date as update_date
        from gl_trans gl
        inner join dim_date_cte dt on dt.calendar_date = gl.gl_date -- Field should reference Financial Tags, picked from gl
        inner join item_ext itm_ext on gl.sku = itm_ext.source_item_identifier  --Joins when a valid SKU  
        left join trade_type_set 
            on gl.trade_type = trade_type_set.trade_type_code
            and gl.document_company = trade_type_set.company_code  --Left join to identify invalid/null Trade Types
        inner join  --should be inner join for valid sku
            allocbasis_sku allobasic
            on gl.sku = allobasic.source_item_identifier
            and date_trunc('month', gl.gl_date) = allobasic.gl_month
            and gl.document_company = allobasic.company_code
        where
            trade_type_set.trade_type_code is null   --this confirms that Trade Type is invalid or null.
            and (
                oc_txn_ledger_amt
                + oc_base_ledger_amt
                + oc_phi_ledger_amt
                + oc_pcomp_ledger_amt
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
            allobasic.trade_type_code,
            itm_ext.product_class_code,
            gl.sku,
            allobasic.item_guid
    ),
   -- select * from scenario2
   /* Scenario 3:There is a trade type but no SKU or invalid SKU, volme based allocation across all SKU's that are under that Trade Type*/
   scenario3 as (
        select
            gl.source_system,
            gl.target_account_identifier,
            gl.account_guid,
            gl.source_object_id,
            gl.account_category,
            gl.business_unit_address_guid,
            gl.gl_date,
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
            sum(oc_txn_ledger_amt * allobasic.perc_trade_type) as oc_txn_ledger_amt,
            sum(oc_base_ledger_amt * allobasic.perc_trade_type) as oc_base_ledger_amt,
            sum(oc_phi_ledger_amt * allobasic.perc_trade_type) as oc_phi_ledger_amt,
            sum(oc_pcomp_ledger_amt * allobasic.perc_trade_type) as oc_pcomp_ledger_amt,
            gl.trade_type as trade_type,  -- Field should reference Financial Tags
            allobasic.product_class_code as product_class,  -- Field should reference Financial Tags,invalid product class, since sku is invalid took this field from GL,confirm this from Mike
            allobasic.source_item_identifier as source_item_identifier,  -- Field should reference Financial Tags, invalid sku
            to_char(allobasic.item_guid) as item_guid,
            'Trade Option 3: Valid Trade Type not SKU' as union_logic,
            current_date as load_date,
            current_date as update_date
        from gl_trans gl
        inner join dim_date_cte dt on dt.calendar_date = gl.gl_date
        inner join trade_type_set --valid trade_type_code
            on gl.trade_type = trade_type_set.trade_type_code
            and gl.document_company = trade_type_set.company_code  --Joins when a valid Trade Type
        inner join
            allocbasis_trade_type allobasic  --valid trade type
            on gl.trade_type = allobasic.trade_type_code
            and date_trunc('month', gl.gl_date) = allobasic.gl_month
            and gl.document_company = allobasic.company_code
        left join item_ext vs on gl.sku = vs.source_item_identifier
        where
           vs.source_item_identifier is null --invalid sku
            and (
                oc_txn_ledger_amt
                + oc_base_ledger_amt
                + oc_phi_ledger_amt
                + oc_pcomp_ledger_amt
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
            gl.trade_type,
            allobasic.product_class_code,
            allobasic.source_item_identifier,
            allobasic.item_guid
   ),
   --select * from scenario3
   /* scenario4:allocation of spend when No Trade Type or SKU information. Volumne based allocation across all Sku's with volume under all Trade Types.*/
      scenario4 as (
        select
            gl.source_system,
            gl.target_account_identifier,
            gl.account_guid,
            gl.source_object_id,
            gl.account_category,
            gl.business_unit_address_guid,
            gl.gl_date,
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
            sum(oc_txn_ledger_amt * allobasic.perc_month) as oc_txn_ledger_amt,
            sum(oc_base_ledger_amt * allobasic.perc_month) as oc_base_ledger_amt,
            sum(oc_phi_ledger_amt * allobasic.perc_month) as oc_phi_ledger_amt,
            sum(oc_pcomp_ledger_amt * allobasic.perc_month) as oc_pcomp_ledger_amt,
            allobasic.trade_type_code as trade_type,  -- Field should reference Financial Tags
            allobasic.product_class_code as product_class,  -- Field should reference Financial Tags
            allobasic.source_item_identifier as source_item_identifier,  -- Field should reference Financial Tags
            allobasic.item_guid as item_guid,
            'Trade Option 4:invalid SKU and Trade Type' as union_logic,
            current_date as load_date,
            current_date as update_date

        from gl_trans gl
        inner join dim_date_cte dt on dt.calendar_date = gl.gl_date
        left join item_ext vs on gl.sku = vs.source_item_identifier  --Left join to identify invalid/null SKU
        left join trade_type_set 
            on gl.trade_type = trade_type_set.trade_type_code
            and gl.document_company = trade_type_set.company_code  --Left join to identify invalid/null Trade Types
        inner join allocbasis_no_sku_no_tradetype allobasic on date_trunc('month', gl.gl_date) = allobasic.gl_month 
        and gl.document_company = allobasic.company_code
        --this join condition is causing cross join.
        where
            (vs.source_item_identifier is null and trade_type_set.trade_type_code is null) --validate this filter clause for case 4

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
            allobasic.trade_type_code,
            allobasic.product_class_code,
            allobasic.source_item_identifier,
            allobasic.item_guid

        ),
        final_union as 
        (
            select * from scenario1
            union all
            select * from scenario2
            union all
            select * from scenario3
            union all 
            select * from scenario4
        ),
        final_d365 as 
        (
        select
    cast(substring(source_system, 1, 10) as text(10)) as source_system ,
    cast(
        substring(target_account_identifier, 1, 255) as text(255)
    ) as target_account_identifier,
    cast(substring(account_guid, 1, 255) as text(255)) as account_guid,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(account_category, 1, 255) as text(255)) as account_category,
    cast(
        substring(business_unit_address_guid, 1, 60) as text(60)
    ) as business_unit_address_guid,
    cast(gl_date as date) as gl_date,
    cast(substring(document_company, 1, 255) as text(255)) as document_company,
    cast(
        substring(source_account_identifier, 1, 255) as text(255)
    ) as source_account_identifier,
    cast(
        substring(source_business_unit_code, 1, 255) as text(255)
    ) as source_business_unit_code,
    cast(substring(base_currency, 1, 10) as text(10)) as base_currency,
    cast(substring(transaction_currency, 1, 10) as text(10)) as transaction_currency,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
    cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
    cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
    cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
    cast(
        substring(source_item_identifier, 1, 255) as text(255)
    ) as source_item_identifier,
    cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
    cast(
        substring(fiscal_year_period_no, 1, 255) as text(255)
    ) as fiscal_year_period_no,
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
    ) as unique_key ,
    'D365' as source_legacy
from final_union
        ),
        final as 
        (
            select 
              source_system,
target_account_identifier,
account_guid,
source_object_id,
business_unit_address_guid,
gl_date,
document_company,
source_account_identifier,
source_business_unit_code,
base_currency,
transaction_currency,
phi_currency,
pcomp_currency,
base_ledger_amt,
txn_ledger_amt,
phi_ledger_amt,
pcomp_ledger_amt,
source_item_identifier,
item_guid,
fiscal_year_period_no,
trade_type,
product_class,
journal_entry_flag,
union_logic,
load_date,
update_date,
unique_key,
account_category,
source_legacy
 from  final_d365
  {% if check_ax_hist_exists( this.schema, this.table ) == 'False' %}
 union all 
 select 
    source_system,
target_account_identifier,
account_guid,
source_object_id,
business_unit_address_guid,
gl_date,
document_company,
source_account_identifier,
source_business_unit_code,
base_currency,
transaction_currency,
phi_currency,
pcomp_currency,
base_ledger_amt,
txn_ledger_amt,
phi_ledger_amt,
pcomp_ledger_amt,
source_item_identifier,
item_guid,
fiscal_year_period_no,
trade_type,
product_class,
journal_entry_flag,
union_logic,
current_date as load_date,
current_date as update_date,
unique_key,
null as account_category,
source_legacy
from ax_hist 
{% endif%}
        )
        select * from final