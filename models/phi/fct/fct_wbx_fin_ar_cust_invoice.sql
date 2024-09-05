{{
    config(
        materialized = env_var('DBT_MAT_INCREMENTAL'),
        tags=["finance","ar"],
        transient = false,
        unique_key = 'UNIQUE_KEY',
        on_schema_change='sync_all_columns',        
        pre_hook=
        """
        {% set now = modules.datetime.datetime.now() %}
        {%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
        {%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}
        {% if day_today == full_load_day %}
        {{ truncate_if_exists(this.schema, this.table) }}
        {% endif %}
        """ 
    )
}}


with int_fact as (
    select *
    from {{ ref("int_f_wbx_fin_ar_cust_invoice") }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

/* Pulls the corresponding AX history across to blend w/ the D365 transactional data.
    For the conv model, the relevant dimension values have been converted, where that is applicable.
*/
old_ax_fact as (
    select * from {{ ref('conv_fct_wbx_fin_ar_cust_invoice') }}
),


int as ( --need to make sure the report columns are in the SAME ORDER
    select
        account_guid,
        ar_custinv_hdr_unique_key,
        base_conv_rt,
        base_currency,
        base_discount_available_amt,
        base_discount_taken_amt,
        base_gross_amt,
        base_nontaxable_amt,
        base_open_amt,
        base_tax_amt,
        base_taxable_amt,
        batch_date,
        batch_number,
        batch_type,
        business_unit_address_guid,
        cleared_date,
        company_code,
        customer_address_number_guid,
        deduction_reason_code,
        discount_due_date,
        document_company,
        document_number,
        document_pay_item,
        document_type,
        foreign_transaction_flag,
        gl_date,
        gl_offset_srccd,
        gl_posted_flag,
        intercompany_flag,
        invoice_closed_date,
        invoice_date,
        item_guid,
        load_date,
        name_txt,
        net_due_date,
        original_document_company,
        original_document_number,
        original_document_pay_item,
        original_document_type,
        pay_status_code,
        payment_identifier,
        payment_instr_desc,
        payment_terms_guid,
        payor_address_number_guid,
        pcomp_conv_rt,
        pcomp_currency,
        pcomp_discount_available_amt,
        pcomp_discount_taken_amt,
        pcomp_gross_amt,
        pcomp_nontaxable_amt,
        pcomp_open_amt,
        pcomp_tax_amt,
        pcomp_taxable_amt,
        phi_conv_rt,
        phi_currency,
        phi_discount_available_amt,
        phi_discount_taken_amt,
        phi_gross_amt,
        phi_nontaxable_amt,
        phi_open_amt,
        phi_tax_amt,
        phi_taxable_amt,
        projected_payment_date,
        quantity,
        reference_txt,
        remark_txt,
        sales_document_company,
        sales_document_number,
        sales_document_suffix,
        sales_document_type,
        service_date,
        source_account_identifier,
        source_business_unit_code,
        --null as source_company_code,
        source_customer_identifier,
        source_date_updated,
        source_document_type,
        source_item_identifier,
        source_payment_instr_code,
        source_payment_terms_code,
        source_payor_identifier,
        source_system,
        source_updated_d_id,
        supplier_invoice_number,
        target_account_identifier,
        target_payment_instr_code,
        transaction_currency,
        transaction_uom,
        txn_conv_rt,
        txn_discount_available_amt,
        txn_discount_taken_amt,
        txn_gross_amt,
        txn_nontaxable_amt,
        txn_open_amt,
        txn_tax_amt,
        txn_taxable_amt,
        unique_key,
        update_date,
        void_date,
        void_flag,
        --null as source_object_id,
        'D365' as source_legacy
    from int_fact
),

ax_hist as (
    select
        a.account_guid,
        a.ar_custinv_hdr_unique_key,
        a.base_conv_rt,
        a.base_currency,
        a.base_discount_available_amt,
        a.base_discount_taken_amt,
        a.base_gross_amt,
        a.base_nontaxable_amt,
        a.base_open_amt,
        a.base_tax_amt,
        a.base_taxable_amt,
        a.batch_date,
        a.batch_number,
        a.batch_type,
        a.business_unit_address_guid,
        a.cleared_date,
        a.company_code,
        a.customer_address_number_guid,
        a.deduction_reason_code,
        a.discount_due_date,
        a.document_company,
        a.document_number,
        a.document_pay_item,
        a.document_type,
        a.foreign_transaction_flag,
        a.gl_date,
        a.gl_offset_srccd,
        a.gl_posted_flag,
        a.intercompany_flag,
        a.invoice_closed_date,
        a.invoice_date,
        a.item_guid,
        a.load_date,
        a.name_txt,
        a.net_due_date,
        a.original_document_company,
        a.original_document_number,
        a.original_document_pay_item,
        a.original_document_type,
        a.pay_status_code,
        a.payment_identifier,
        a.payment_instr_desc,
        a.payment_terms_guid,
        a.payor_address_number_guid,
        a.pcomp_conv_rt,
        a.pcomp_currency,
        a.pcomp_discount_available_amt,
        a.pcomp_discount_taken_amt,
        a.pcomp_gross_amt,
        a.pcomp_nontaxable_amt,
        a.pcomp_open_amt,
        a.pcomp_tax_amt,
        a.pcomp_taxable_amt,
        a.phi_conv_rt,
        a.phi_currency,
        a.phi_discount_available_amt,
        a.phi_discount_taken_amt,
        a.phi_gross_amt,
        a.phi_nontaxable_amt,
        a.phi_open_amt,
        a.phi_tax_amt,
        a.phi_taxable_amt,
        a.projected_payment_date,
        a.quantity,
        a.reference_txt,
        a.remark_txt,
        a.sales_document_company,
        a.sales_document_number,
        a.sales_document_suffix,
        a.sales_document_type,
        a.service_date,
        a.source_account_identifier,
        a.source_business_unit_code,
        --a.source_company_code,
        a.source_customer_identifier,
        a.source_date_updated,
        a.source_document_type,
        a.source_item_identifier,
        a.source_payment_instr_code,
        a.source_payment_terms_code,
        a.source_payor_identifier,
        a.source_system,
        a.source_updated_d_id,
        a.supplier_invoice_number,
        a.target_account_identifier,
        a.target_payment_instr_code,
        a.transaction_currency,
        a.transaction_uom,
        a.txn_conv_rt,
        a.txn_discount_available_amt,
        a.txn_discount_taken_amt,
        a.txn_gross_amt,
        a.txn_nontaxable_amt,
        a.txn_open_amt,
        a.txn_tax_amt,
        a.txn_taxable_amt,
        a.unique_key,
        a.update_date,
        a.void_date,
        a.void_flag,
        --a.source_object_id,
        'AX' as source_legacy
    from old_ax_fact as a
    left join int as b on a.unique_key = b.unique_key
    where b.source_system is null
),

final as (
    select * from int
    union
    select * from ax_hist
)

select
    --cast(source_object_id as varchar(255)) as source_object_id, --not in prod model
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(document_number, 1, 255) as text(255))
        as document_number,
    cast(substring(source_document_type, 1, 255) as text(255))
        as source_document_type,
    cast(substring(document_type, 1, 255) as text(255)) as document_type,
    cast(substring(document_company, 1, 255) as text(255))
        as document_company,
    cast(substring(document_pay_item, 1, 255) as text(255))
        as document_pay_item,
    cast(substring(source_customer_identifier, 1, 255) as text(255))
        as source_customer_identifier,
    cast(substring(customer_address_number_guid, 1, 255) as text(255))
        as customer_address_number_guid,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(invoice_date as timestamp_ntz(9)) as invoice_date,
    cast(substring(company_code, 1, 255) as text(255)) as company_code,
    cast(substring(gl_offset_srccd, 1, 255) as text(255))
        as gl_offset_srccd,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(substring(source_payor_identifier, 1, 255) as text(255))
        as source_payor_identifier,
    cast(substring(payor_address_number_guid, 1, 255) as text(255))
        as payor_address_number_guid,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(substring(pay_status_code, 1, 20) as text(20))
        as pay_status_code,
    cast(substring(foreign_transaction_flag, 1, 20) as text(20))
        as foreign_transaction_flag,
    cast(service_date as timestamp_ntz(9)) as service_date,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(substring(business_unit_address_guid, 1, 255) as text(255))
        as business_unit_address_guid,
    cast(
        substring(
            source_payment_terms_code,
            1, 255
        ) as text(255)
    ) as source_payment_terms_code,
    cast(substring(payment_terms_guid,1,255) as text(255))
        as payment_terms_guid,
    -- cast(substring(source_company_code, 1, 255) as text(255))
    --    as source_company_code,
    cast(substring(account_guid, 1, 255) as text(255)) as account_guid,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(net_due_date as timestamp_ntz(9)) as net_due_date,
    cast(discount_due_date as timestamp_ntz(9)) as discount_due_date,
    cast(projected_payment_date as timestamp_ntz(9))
        as projected_payment_date,
    cast(substring(original_document_number, 1, 255) as text(255))
        as original_document_number,
    cast(substring(original_document_type, 1, 255) as text(255))
        as original_document_type,
    cast(substring(original_document_company, 1, 255) as text(255))
        as original_document_company,
    cast(substring(original_document_pay_item, 1, 255) as text(255))
        as original_document_pay_item,
    cast(substring(supplier_invoice_number, 1, 255) as text(255))
        as supplier_invoice_number,
    cast(substring(sales_document_number, 1, 255) as text(255))
        as sales_document_number,
    cast(substring(sales_document_type, 1, 255) as text(255))
        as sales_document_type,
    cast(substring(sales_document_company, 1, 255) as text(255))
        as sales_document_company,
    cast(substring(sales_document_suffix, 1, 255) as text(255))
        as sales_document_suffix,
    cast(cleared_date as timestamp_ntz(9)) as cleared_date,
    cast(substring(reference_txt, 1, 255) as text(255)) as reference_txt,
    cast(substring(remark_txt, 1, 255) as text(255)) as remark_txt,
    cast(substring(name_txt, 1, 255) as text(255)) as name_txt,
    cast(substring(source_item_identifier, 1, 255) as text(255))
        as source_item_identifier,
    cast(substring(item_guid, 1, 255) as text(255)) as item_guid,
    cast(quantity as number(38, 10)) as quantity,
    cast(substring(transaction_uom, 1, 255) as text(255))
        as transaction_uom,
    cast(substring(source_payment_instr_code, 1, 255) as text(255))
        as source_payment_instr_code,
    cast(substring(target_payment_instr_code, 1, 255) as text(255))
        as target_payment_instr_code,
    cast(substring(payment_instr_desc, 1, 255) as text(255))
        as payment_instr_desc,
    cast(void_date as timestamp_ntz(9)) as void_date,
    cast(substring(void_flag, 1, 255) as text(255)) as void_flag,
    cast(substring(payment_identifier, 1, 255) as text(255))
        as payment_identifier,
    cast(invoice_closed_date as timestamp_ntz(9)) as invoice_closed_date,
    cast(substring(deduction_reason_code, 1, 255) as text(255))
        as deduction_reason_code,
    cast(substring(transaction_currency, 1, 255) as text(255))
        as transaction_currency,
    cast(substring(base_currency, 1, 255) as text(255)) as base_currency,
    cast(substring(phi_currency, 1, 255) as text(255)) as phi_currency,
    cast(substring(pcomp_currency, 1, 255) as text(255)) as pcomp_currency,
    cast(txn_conv_rt as number(29, 9)) as txn_conv_rt,
    cast(base_conv_rt as number(29, 9)) as base_conv_rt,
    cast(phi_conv_rt as number(29, 9)) as phi_conv_rt,
    cast(pcomp_conv_rt as number(29, 9)) as pcomp_conv_rt,
    cast(txn_gross_amt as number(38, 10)) as txn_gross_amt,
    cast(txn_open_amt as number(38, 10)) as txn_open_amt,
    cast(txn_discount_available_amt as number(38, 10))
        as txn_discount_available_amt,
    cast(txn_discount_taken_amt as number(38, 10))
        as txn_discount_taken_amt,
    cast(txn_taxable_amt as number(38, 10)) as txn_taxable_amt,
    cast(txn_nontaxable_amt as number(38, 10)) as txn_nontaxable_amt,
    cast(txn_tax_amt as number(38, 10)) as txn_tax_amt,
    cast(base_gross_amt as number(38, 10)) as base_gross_amt,
    cast(base_open_amt as number(38, 10)) as base_open_amt,
    cast(base_discount_available_amt as number(38, 10))
        as base_discount_available_amt,
    cast(base_discount_taken_amt as number(38, 10))
        as base_discount_taken_amt,
    cast(base_taxable_amt as number(38, 10)) as base_taxable_amt,
    cast(base_nontaxable_amt as number(38, 10)) as base_nontaxable_amt,
    cast(base_tax_amt as number(38, 10)) as base_tax_amt,
    cast(phi_gross_amt as number(38, 10)) as phi_gross_amt,
    cast(phi_open_amt as number(38, 10)) as phi_open_amt,
    cast(phi_discount_available_amt as number(38, 10))
        as phi_discount_available_amt,
    cast(phi_discount_taken_amt as number(38, 10))
        as phi_discount_taken_amt,
    cast(phi_taxable_amt as number(38, 10)) as phi_taxable_amt,
    cast(phi_nontaxable_amt as number(38, 10)) as phi_nontaxable_amt,
    cast(phi_tax_amt as number(38, 10)) as phi_tax_amt,
    cast(pcomp_gross_amt as number(38, 10)) as pcomp_gross_amt,
    cast(pcomp_open_amt as number(38, 10)) as pcomp_open_amt,
    cast(pcomp_discount_available_amt as number(38, 10))
        as pcomp_discount_available_amt,
    cast(pcomp_discount_taken_amt as number(38, 10))
        as pcomp_discount_taken_amt,
    cast(pcomp_taxable_amt as number(38, 10)) as pcomp_taxable_amt,
    cast(pcomp_nontaxable_amt as number(38, 10)) as pcomp_nontaxable_amt,
    cast(pcomp_tax_amt as number(38, 10)) as pcomp_tax_amt,
    cast(substring(intercompany_flag, 1, 1) as text(1))
        as intercompany_flag,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(substring(batch_type, 1, 4) as text(4)) as batch_type,
    cast(substring(batch_number, 1, 255) as text(255)) as batch_number,
    cast(batch_date as timestamp_ntz(9)) as batch_date,
    cast(substring(ar_custinv_hdr_unique_key, 1, 255) as text(255))
        as ar_custinv_hdr_unique_key,
    cast(substring(unique_key, 1, 255) as text(255)) as unique_key,
    cast(substring(source_legacy, 1, 255) as text(255)) as source_legacy
from final