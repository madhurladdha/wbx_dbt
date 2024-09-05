{{
    config(
        materialized=env_var("DBT_MAT_INCREMENTAL"),
        tags=["finance","gl","gl_trans"],
        unique_key="unique_key",
        snowflake_warehouse=env_var('DBT_WBX_SF_WH'),
        on_schema_change="sync_all_columns",
        incremental_strategy="delete+insert",
        pre_hook="""
            {% set now = modules.datetime.datetime.now() %}
            {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
            {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
            {% if day_today == full_load_day %}
            {{ truncate_if_exists(this.schema, this.table) }}
            {% endif %}
            """,
    )
}}

/* The GL Trans Fact, as part of the D365 conversion from AX now combines (unions) the Converted AX History data w/ the "new" data from D365 to make
    up the full historical set for the given Fact.
    If a given unique row (primary key) exists in both sets that means it was converted by the D365 team in the transaction system.  In such cases
    the code here is meant to ensure that only the D365 row is pulled through and materialized.

    Note that GL Trans AX History data is quite large at over 100M rows and so that can and does slow down the model.  It also means that the load strategy used in the
    config block is critical to be handled correctly.
*/


with int_fact as (
    select *
    from {{ ref("int_f_wbx_fin_gl_trans") }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

old_fct as (
    select *
    from
        {{ ref('conv_fct_wbx_fin_gl_trans') }}
/*this section brings ax prod data which will become  static table post BR2 cutover */
),


int1 as (
    select
        unique_key,
        source_system,
        document_company,
        source_document_type,
        document_type,
        document_number,
        source_account_identifier,
        gl_date,
        journal_line_number,
        gl_extension_code,
        source_business_unit_code,
        void_flag,
        payment_trans_date,
        document_pay_item,
        payment_number,
        payment_identifier,
        base_currency,
        target_account_identifier,
        account_guid,
        source_ledger_type,
        ledger_type,
        gl_posted_flag,
        batch_number,
        batch_type,
        batch_date,
        source_address_number,
        address_guid,
        company_code,
        source_object_id,
        source_subsidiary_id,
        source_company_code,
        business_unit_address_guid,
        txn_ledger_amt,
        transaction_currency,
        base_ledger_amt,
        quantity,
        supplier_invoice_number,
        invoice_date,
        account_cat21_code,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        explanation_txt,
        remark_txt,
        reference1_txt,
        reference2_txt,
        reference3_txt,
        transaction_uom,
        department,
        caf_no,
        plant,
        phi_ledger_amt,
        pcomp_ledger_amt,
        txn_conv_rt,
        phi_currency,
        phi_conv_rt,
        pcomp_currency,
        pcomp_conv_rt,
        product_dim,
        customer_dim,
        oc_txn_ledger_amt,
        oc_base_ledger_amt,
        oc_pcomp_ledger_amt,
        oc_phi_ledger_amt,
        journal_category,
        transledger_journal_number,
        ledger_account,
        trade_type,
        sku,
        promo_term_code,
        main_account,
        main_account_name,
        cost_center,
        cost_center_name,
        product_class,
        product_class_name,
        us_account,
        us_account_name,
        account_category,
        account_type,
        'D365' as source_legacy
        /* added source_legacy flag to identify if data is from old Ax history  or D365 source */
    from int_fact
),

ax_hist as (
    select
        a.unique_key,
        a.source_system,
        a.document_company,
        a.source_document_type,
        a.document_type,
        a.document_number,
        a.source_account_identifier,
        a.gl_date,
        a.journal_line_number,
        a.gl_extension_code,
        a.source_business_unit_code,
        a.void_flag,
        a.payment_trans_date,
        a.document_pay_item,
        a.payment_number,
        a.payment_identifier,
        a.base_currency,
        a.target_account_identifier,
        a.account_guid,
        a.source_ledger_type,
        a.ledger_type,
        a.gl_posted_flag,
        a.batch_number,
        a.batch_type,
        a.batch_date,
        a.source_address_number,
        a.address_guid,
        a.company_code,
        a.source_object_id,
        a.source_subsidiary_id,
        a.source_company_code,
        a.business_unit_address_guid,
        a.txn_ledger_amt,
        a.transaction_currency,
        a.base_ledger_amt,
        a.quantity,
        a.supplier_invoice_number,
        a.invoice_date,
        a.account_cat21_code,
        a.source_date_updated,
        a.load_date,
        a.update_date,
        a.source_updated_d_id,
        a.explanation_txt,
        a.remark_txt,
        a.reference1_txt,
        a.reference2_txt,
        a.reference3_txt,
        a.transaction_uom,
        a.department,
        a.caf_no,
        a.plant,
        a.phi_ledger_amt,
        a.pcomp_ledger_amt,
        a.txn_conv_rt,
        a.phi_currency,
        a.phi_conv_rt,
        a.pcomp_currency,
        a.pcomp_conv_rt,
        a.product_dim,
        a.customer_dim,
        a.oc_txn_ledger_amt,
        a.oc_base_ledger_amt,
        a.oc_pcomp_ledger_amt,
        a.oc_phi_ledger_amt,
        a.journal_category,
        null as transledger_journal_number,
        null as ledger_account,
        null as trade_type,
        null as sku,
        null as promo_term_code,
        null as main_account,
        null as main_account_name,
        null as cost_center,
        null as cost_center_name,
        null as product_class,
        null as product_class_name,
        null as us_account,
        null as us_account_name,
        null as account_category,
        null as account_type,
        a.source_legacy as source_legacy
    /* Only want the AX History data IF the unique key is NOT in the D365 data from the int model.
    */
    from old_fct as a
    left join int1 as b on a.unique_key = b.unique_key where b.source_system is null
),

final as (
    select * from int1
    union
    select * from ax_hist
)

select
    cast(unique_key as text(255)) as unique_key,
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(document_company, 1, 20) as text(20)) as document_company,
    cast(substring(source_document_type, 1, 20) as text(20))
        as source_document_type,
    cast(substring(document_type, 1, 20) as text(20)) as document_type,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(journal_line_number as number(38, 0)) as journal_line_number,
    cast(substring(gl_extension_code, 1, 2) as text(2)) as gl_extension_code,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(substring(void_flag, 1, 4) as text(4)) as void_flag,
    cast(payment_trans_date as timestamp_ntz(9)) as payment_trans_date,
    cast(substring(document_pay_item, 1, 6) as text(6)) as document_pay_item,
    cast(substring(payment_number, 1, 255) as text(255)) as payment_number,
    cast(substring(payment_identifier, 1, 255) as text(255))
        as payment_identifier,
    cast(substring(base_currency, 1, 20) as text(20)) as base_currency,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(account_guid as text(255)) as account_guid,
    cast(substring(source_ledger_type, 1, 255) as text(255))
        as source_ledger_type,
    cast(substring(ledger_type, 1, 255) as text(255)) as ledger_type,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(substring(batch_number, 1, 255) as text(255)) as batch_number,
    cast(substring(batch_type, 1, 4) as text(4)) as batch_type,
    cast(batch_date as timestamp_ntz(9)) as batch_date,
    cast(substring(source_address_number, 1, 255) as text(255))
        as source_address_number,
    cast(address_guid as text(255)) as address_guid,
    cast(substring(company_code, 1, 20) as text(20)) as company_code,
    cast(substring(source_object_id, 1, 255) as text(255)) as source_object_id,
    cast(substring(source_subsidiary_id, 1, 255) as text(255))
        as source_subsidary_id,
    cast(substring(source_company_code, 1, 255) as text(255))
        as source_company_code,
    cast(business_unit_address_guid as text(255)) as business_unit_address_guid,
    cast(txn_ledger_amt as number(38, 10)) as txn_ledger_amt,
    cast(substring(transaction_currency, 1, 20) as text(20))
        as transaction_currency,
    cast(base_ledger_amt as number(38, 10)) as base_ledger_amt,
    cast(quantity as number(38, 10)) as quantity,
    cast(substring(supplier_invoice_number, 1, 255) as text(255))
        as supplier_invoice_number,
    cast(invoice_date as timestamp_ntz(9)) as invoice_date,
    cast(substring(account_cat21_code, 1, 255) as text(255))
        as account_cat21_code,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(substring(explanation_txt, 1, 255) as text(255)) as explanation_txt,
    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
    cast(substring(reference1_txt, 1, 255) as text(255)) as reference1_txt,
    cast(substring(reference2_txt, 1, 255) as text(255)) as reference2_txt,
    cast(substring(reference3_txt, 1, 255) as text(255)) as reference3_txt,
    cast(substring(transaction_uom, 1, 255) as text(255)) as transaction_uom,
    cast(substring(department, 1, 30) as text(30)) as department,
    cast(substring(caf_no, 1, 30) as text(30)) as caf_no,
    cast(substring(plant, 1, 30) as text(30)) as plant,
    cast(phi_ledger_amt as number(38, 10)) as phi_ledger_amt,
    cast(pcomp_ledger_amt as number(38, 10)) as pcomp_ledger_amt,
    cast(txn_conv_rt as number(38, 10)) as txn_conv_rt,
    cast(substring(phi_currency, 1, 10) as text(10)) as phi_currency,
    cast(phi_conv_rt as number(38, 10)) as phi_conv_rt,
    cast(substring(pcomp_currency, 1, 10) as text(10)) as pcomp_currency,
    cast(pcomp_conv_rt as number(38, 10)) as pcomp_conv_rt,
    cast(substring(product_dim, 1, 255) as text(255)) as product_dim,
    cast(substring(customer_dim, 1, 255) as text(255)) as customer_dim,
    cast(oc_txn_ledger_amt as number(38, 10)) as oc_txn_ledger_amt,
    cast(oc_base_ledger_amt as number(38, 10)) as oc_base_ledger_amt,
    cast(oc_pcomp_ledger_amt as number(38, 10)) as oc_pcomp_ledger_amt,
    cast(oc_phi_ledger_amt as number(38, 10)) as oc_phi_ledger_amt,
    cast(substring(journal_category, 1, 20) as text(20)) as journal_category,
    cast(substring(transledger_journal_number, 1, 255) as text(255))
        as transledger_journal_number,
    cast(substring(ledger_account, 1, 255) as text(255)) as ledger_account,
    cast(substring(trade_type, 1, 255) as text(255)) as trade_type,
    cast(substring(sku, 1, 255) as text(255)) as sku,
    cast(substring(promo_term_code, 1, 255) as text(255)) as promo_term_code,
    cast(substring(main_account, 1, 255) as text(255)) as main_account,
    cast(substring(main_account_name, 1, 255) as text(255))
        as main_account_name,
    cast(substring(cost_center, 1, 255) as text(255)) as cost_center,
    cast(substring(cost_center_name, 1, 255) as text(255)) as cost_center_name,
    cast(substring(product_class, 1, 255) as text(255)) as product_class,
    cast(substring(product_class_name, 1, 255) as text(255))
        as product_class_name,
    cast(substring(us_account, 1, 255) as text(255)) as us_account,
    cast(substring(us_account_name, 1, 255) as text(255)) as us_account_name,
    cast(substring(account_category, 1, 255) as text(255)) as account_category,
    cast(substring(account_type, 1, 255) as text(255)) as account_type,
    cast(source_legacy as text(15)) as source_legacy
from final




