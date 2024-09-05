{{
    config(
        materialized = env_var('DBT_MAT_INCREMENTAL'),
        tags=["finance","ar"],
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


with int_fact as (
    select *
    from {{ ref("int_f_wbx_fin_ar_custinv_hdr") }} qualify
        row_number() over (partition by unique_key order by 1) = 1
),

/* Pulls the corresponding AX history across to blend w/ the D365 transactional data.
    For the conv model, the relevant dimension values have been converted, where that is applicable.
*/
old_ax_fact as (
    select * from {{ ref('conv_fct_wbx_fin_ar_custinv_hdr') }}
),

int as (
    select
        source_system,
        document_number,
        source_document_type,
        document_type,
        document_company,
        source_customer_identifier,
        customer_address_number_guid,
        gl_date,
        invoice_date,
        company_code,
        source_account_identifier,
        target_account_identifier,
        account_guid,
        gl_posted_flag,
        pay_status_code,
        foreign_transaction_flag,
        service_date,
        source_business_unit_code,
        business_unit_address_guid,
        source_payment_terms_code,
        payment_terms_guid,
        net_due_date,
        discount_due_date,
        projected_payment_date,
        sales_document_number,
        sales_document_type,
        sales_document_company,
        cleared_date,
        reference_txt,
        name_txt,
        source_payment_instr_code,
        target_payment_instr_code,
        payment_instr_desc,
        payment_identifier,
        invoice_closed_date,
        transaction_currency,
        base_currency,
        phi_currency,
        pcomp_currency,
        txn_conv_rt,
        base_conv_rt,
        phi_conv_rt,
        pcomp_conv_rt,
        txn_gross_amt,
        txn_open_amt,
        txn_discount_available_amt,
        txn_discount_taken_amt,
        txn_taxable_amt,
        txn_nontaxable_amt,
        txn_tax_amt,
        txn_purchase_charge_amt,
        base_gross_amt,
        base_open_amt,
        base_discount_available_amt,
        base_discount_taken_amt,
        base_taxable_amt,
        base_nontaxable_amt,
        base_tax_amt,
        base_purchase_charge_amt,
        phi_gross_amt,
        phi_open_amt,
        phi_discount_available_amt,
        phi_discount_taken_amt,
        phi_taxable_amt,
        phi_nontaxable_amt,
        phi_tax_amt,
        phi_purchase_charge_amt,
        pcomp_gross_amt,
        pcomp_open_amt,
        pcomp_discount_available_amt,
        pcomp_discount_taken_amt,
        pcomp_taxable_amt,
        pcomp_nontaxable_amt,
        pcomp_tax_amt,
        pcomp_purchase_charge_amt,
        intercompany_flag,
        source_date_updated,
        load_date,
        update_date,
        source_updated_d_id,
        --null as source_object_id,
        --null as source_company_code,
        unique_key,
        'D365' as source_legacy
    from int_fact
),

ax_hist as (
    select
        a.source_system,
        a.document_number,
        a.source_document_type,
        a.document_type,
        a.document_company,
        a.source_customer_identifier,
        a.customer_address_number_guid,
        a.gl_date,
        a.invoice_date,
        a.company_code,
        a.source_account_identifier,
        a.target_account_identifier,
        a.account_guid,
        a.gl_posted_flag,
        a.pay_status_code,
        a.foreign_transaction_flag,
        a.service_date,
        a.source_business_unit_code,
        a.business_unit_address_guid,
        a.source_payment_terms_code,
        a.payment_terms_guid,
        a.net_due_date,
        a.discount_due_date,
        a.projected_payment_date,
        a.sales_document_number,
        a.sales_document_type,
        a.sales_document_company,
        a.cleared_date,
        a.reference_txt,
        a.name_txt,
        a.source_payment_instr_code,
        a.target_payment_instr_code,
        a.payment_instr_desc,
        a.payment_identifier,
        a.invoice_closed_date,
        a.transaction_currency,
        a.base_currency,
        a.phi_currency,
        a.pcomp_currency,
        a.txn_conv_rt,
        a.base_conv_rt,
        a.phi_conv_rt,
        a.pcomp_conv_rt,
        a.txn_gross_amt,
        a.txn_open_amt,
        a.txn_discount_available_amt,
        a.txn_discount_taken_amt,
        a.txn_taxable_amt,
        a.txn_nontaxable_amt,
        a.txn_tax_amt,
        a.txn_purchase_charge_amt,
        a.base_gross_amt,
        a.base_open_amt,
        a.base_discount_available_amt,
        a.base_discount_taken_amt,
        a.base_taxable_amt,
        a.base_nontaxable_amt,
        a.base_tax_amt,
        a.base_purchase_charge_amt,
        a.phi_gross_amt,
        a.phi_open_amt,
        a.phi_discount_available_amt,
        a.phi_discount_taken_amt,
        a.phi_taxable_amt,
        a.phi_nontaxable_amt,
        a.phi_tax_amt,
        a.phi_purchase_charge_amt,
        a.pcomp_gross_amt,
        a.pcomp_open_amt,
        a.pcomp_discount_available_amt,
        a.pcomp_discount_taken_amt,
        a.pcomp_taxable_amt,
        a.pcomp_nontaxable_amt,
        a.pcomp_tax_amt,
        a.pcomp_purchase_charge_amt,
        a.intercompany_flag,
        a.source_date_updated,
        a.load_date,
        a.update_date,
        a.source_updated_d_id,
        a.unique_key,
        --a.source_object_id,
        --a.source_company_code,
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

select --update to reflect the list above. 
    cast(substring(source_system, 1, 255) as text(255)) as source_system,
    cast(substring(document_number, 1, 255) as text(255)) as document_number,
    cast(substring(source_document_type, 1, 255) as text(255))
        as source_document_type,
    cast(substring(document_type, 1, 255) as text(255)) as document_type,
    cast(substring(document_company, 1, 255) as text(255)) as document_company,
    cast(substring(source_customer_identifier, 1, 255) as text(255))
        as source_customer_identifier,
    cast(substring(customer_address_number_guid, 1, 255) as text(255))
        as customer_address_number_guid,
    cast(gl_date as timestamp_ntz(9)) as gl_date,
    cast(invoice_date as timestamp_ntz(9)) as invoice_date,
    cast(substring(company_code, 1, 255) as text(255)) as company_code,
    cast(substring(target_account_identifier, 1, 255) as text(255))
        as target_account_identifier,
    cast(substring(gl_posted_flag, 1, 6) as text(6)) as gl_posted_flag,
    cast(substring(pay_status_code, 1, 20) as text(20)) as pay_status_code,
    cast(substring(foreign_transaction_flag, 1, 20) as text(20))
        as foreign_transaction_flag,
    cast(service_date as timestamp_ntz(9)) as service_date,
    cast(substring(source_business_unit_code, 1, 255) as text(255))
        as source_business_unit_code,
    cast(substring(business_unit_address_guid, 1, 255) as text(255))
        as business_unit_address_guid,
    cast(substring(source_payment_terms_code, 1, 255) as text(255))
        as source_payment_terms_code,
    cast(substring(payment_terms_guid, 1, 255) as text(255))
        as payment_terms_guid,
    cast(substring(account_guid, 1, 255) as text(255)) as account_guid,
    cast(substring(source_account_identifier, 1, 255) as text(255))
        as source_account_identifier,
    cast(net_due_date as timestamp_ntz(9)) as net_due_date,
    cast(discount_due_date as timestamp_ntz(9)) as discount_due_date,
    cast(projected_payment_date as timestamp_ntz(9)) as projected_payment_date,
    cast(substring(sales_document_number, 1, 255) as text(255))
        as sales_document_number,
    cast(substring(sales_document_type, 1, 255) as text(255))
        as sales_document_type,
    cast(substring(sales_document_company, 1, 255) as text(255))
        as sales_document_company,
    cast(cleared_date as timestamp_ntz(9)) as cleared_date,
    cast(substring(reference_txt, 1, 255) as text(255)) as reference_txt,
    cast(substring(name_txt, 1, 255) as text(255)) as name_txt,
    cast(substring(source_payment_instr_code, 1, 255) as text(255))
        as source_payment_instr_code,
    cast(substring(target_payment_instr_code, 1, 255) as text(255))
        as target_payment_instr_code,
    cast(substring(payment_instr_desc, 1, 255) as text(255))
        as payment_instr_desc,
    cast(substring(payment_identifier, 1, 255) as text(255))
        as payment_identifier,
    cast(invoice_closed_date as timestamp_ntz(9)) as invoice_closed_date,
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
    cast(txn_discount_taken_amt as number(38, 10)) as txn_discount_taken_amt,
    cast(txn_taxable_amt as number(38, 10)) as txn_taxable_amt,
    cast(txn_nontaxable_amt as number(38, 10)) as txn_nontaxable_amt,
    cast(txn_tax_amt as number(38, 10)) as txn_tax_amt,
    cast(txn_purchase_charge_amt as number(38, 10)) as txn_purchase_charge_amt,
    cast(base_gross_amt as number(38, 10)) as base_gross_amt,
    cast(base_open_amt as number(38, 10)) as base_open_amt,
    cast(base_discount_available_amt as number(38, 10))
        as base_discount_available_amt,
    cast(base_discount_taken_amt as number(38, 10)) as base_discount_taken_amt,
    cast(base_taxable_amt as number(38, 10)) as base_taxable_amt,
    cast(base_nontaxable_amt as number(38, 10)) as base_nontaxable_amt,
    cast(base_tax_amt as number(38, 10)) as base_tax_amt,
    cast(base_purchase_charge_amt as number(38, 10))
        as base_purchase_charge_amt,
    cast(phi_gross_amt as number(38, 10)) as phi_gross_amt,
    cast(phi_open_amt as number(38, 10)) as phi_open_amt,
    cast(phi_discount_available_amt as number(38, 10))
        as phi_discount_available_amt,
    cast(phi_discount_taken_amt as number(38, 10))
        as phi_discount_taken_amt,
    cast(phi_taxable_amt as number(38, 10)) as phi_taxable_amt,
    cast(phi_nontaxable_amt as number(38, 10)) as phi_nontaxable_amt,
    cast(phi_tax_amt as number(38, 10)) as phi_tax_amt,
    cast(phi_purchase_charge_amt as number(38, 10))
        as phi_purchase_charge_amt,
    cast(pcomp_gross_amt as number(38, 10)) as pcomp_gross_amt,
    cast(pcomp_open_amt as number(38, 10)) as pcomp_open_amt,
    cast(pcomp_discount_available_amt as number(38, 10))
        as pcomp_discount_available_amt,
    cast(pcomp_discount_taken_amt as number(38, 10))
        as pcomp_discount_taken_amt,
    cast(pcomp_taxable_amt as number(38, 10)) as pcomp_taxable_amt,
    cast(pcomp_nontaxable_amt as number(38, 10)) as pcomp_nontaxable_amt,
    cast(pcomp_tax_amt as number(38, 10)) as pcomp_tax_amt,
    cast(pcomp_purchase_charge_amt as number(38, 10))
        as pcomp_purchase_charge_amt,
    cast(substring(intercompany_flag, 1, 1) as text(1))
        as intercompany_flag,
    cast(source_date_updated as timestamp_ntz(9)) as source_date_updated,
    cast(load_date as timestamp_ntz(9)) as load_date,
    cast(update_date as timestamp_ntz(9)) as update_date,
    cast(source_updated_d_id as number(38, 0)) as source_updated_d_id,
    cast(unique_key as varchar(255)) as unique_key
from final