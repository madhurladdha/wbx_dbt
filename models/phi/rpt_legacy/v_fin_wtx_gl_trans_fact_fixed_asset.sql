{{ config(tags=["finance", "gl", "trans", "fixed_asset"]) }}

with
    fin_wtx_gl_trans_fact as (select * from {{ ref("fct_wbx_fin_gl_trans") }}),
    dim_date as (select * from {{ ref("src_dim_date") }}),
    ref_hierarchy_xref as (select * from {{ ref("xref_wbx_hierarchy") }}),
    fin_account_dim as (select * from {{ ref("dim_wbx_account") }}),
    adr_plant_dc_master_dim as (select * from {{ ref("dim_wbx_plant_dc") }}),
    adr_company_master_dim as (select * from {{ ref("dim_wbx_company") }})

select
    glt.source_system,
    glt.document_company,
    glt.source_document_type,
    glt.document_type,
    glt.document_number as journal_id,
    glt.source_account_identifier,
    trunc(glt.gl_date, 'DAY') as gl_date,
    glt.journal_line_number as journal_line_id,
    glt.gl_extension_code,
    glt.source_business_unit_code as cost_center,
    glt.void_flag,
    glt.payment_trans_date,
    glt.document_pay_item,
    glt.payment_number,
    glt.payment_identifier,
    glt.base_currency,
    glt.target_account_identifier,
    glt.source_ledger_type,
    glt.ledger_type,
    glt.gl_posted_flag,
    glt.batch_number,
    glt.batch_type,
    glt.batch_date,
    glt.source_address_number,
    glt.company_code,
    glt.source_object_id,
    glt.source_subsidary_id,
    glt.source_company_code,
    glt.txn_ledger_amt,
    glt.transaction_currency,
    glt.base_ledger_amt,
    glt.quantity,
    glt.supplier_invoice_number,
    glt.invoice_date,
    glt.account_cat21_code,
    glt.source_date_updated,
    glt.load_date,
    glt.update_date,
    glt.source_updated_d_id,
    glt.explanation_txt,
    glt.remark_txt as transaction_description,
    substr(
        glt.remark_txt, 1, regexp_instr(glt.remark_txt, 'Journal Cat') - 2
    ) as journal_line_description,
    glt.reference1_txt,
    glt.reference2_txt,
    glt.reference3_txt,
    glt.transaction_uom,
    glt.department as department_code,
    glt.caf_no,
    glt.plant,
    glt.phi_ledger_amt,
    glt.pcomp_ledger_amt,
    glt.txn_conv_rt,
    glt.phi_currency,
    glt.phi_conv_rt,
    glt.pcomp_currency,
    glt.pcomp_conv_rt,

    dd.report_fiscal_year_period_no as fiscal_period,
    h.desc_1,
    h.desc_2,
    h.desc_3,
    h.desc_4,
    h.desc_5,
    h.desc_6,
    h.desc_7,
    h.desc_8,
    h.desc_9,
    h.desc_10,

    to_char(fin.source_object_id) as account,
    upper(fin.account_description) as account_description,

    pl.business_unit_name as cost_center_name,
    pl.operating_company as operating_company,

    cmp.company_name

from fin_wtx_gl_trans_fact glt

inner join dim_date dd on glt.gl_date = dd.calendar_date

inner join
    ref_hierarchy_xref h
    on glt.target_account_identifier = h.tagetik_account
    and h.source_system = 'TAGETIK_ACCOUNT'

left join
    fin_account_dim fin
    on fin.account_guid = glt.account_guid
    and fin.source_system = glt.source_system

left join
    adr_plant_dc_master_dim pl
    on pl.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    and glt.source_business_unit_code = trim(pl.source_business_unit_code)

left join
    adr_company_master_dim cmp
    on cmp.company_code = glt.company_code
    and cmp.source_system = glt.source_system

where
    glt.caf_no is not null
    and (glt.company_code = 'WBX' or glt.company_code = 'RFL')
    and glt.source_object_id in (122900, 122901)
    and glt.source_document_type != 'FAA'
    and glt.source_document_type != 'FAU'
