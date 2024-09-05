with
    old_fct as (
        select *
        from {{ source("FACTS_FOR_COMPARE", "fin_wtx_gl_fact_agg") }}
        where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    ),
converted_fct as (
    select
source_system  ,
document_company  ,
source_document_type  ,
document_type  ,
document_number  ,
source_account_identifier  ,
gl_date ,
fiscal_year  ,
fiscal_year_period_no  ,
source_business_unit_code  ,
source_subledger_identifier  ,
source_subledger_type  ,
void_flag  ,
transaction_currency  ,
target_account_identifier  ,
account_guid  ,
source_ledger_type  ,
ledger_type  ,
batch_number  ,
batch_type  ,
batch_date  ,
source_address_number  ,
address_guid  ,
company_code  ,
source_object_id  ,
source_subsidary_id  ,
source_company_code  ,
business_unit_address_guid  ,
subledger_guid  ,
subledger_type_desc  ,
base_currency  ,
base_ledger_amt  ,
txn_currency  ,
txn_conv_rt  ,
txn_ledger_amt  ,
phi_currency  ,
phi_conv_rt  ,
phi_ledger_amt  ,
pcomp_currency  ,
pcomp_conv_rt  ,
pcomp_ledger_amt  ,
quantity  ,
transaction_uom  ,
supplier_invoice_number  ,
invoice_date  ,
account_cat21_code  ,
source_date_updated  ,
load_date  ,
update_date  ,
source_updated_d_id  ,
explanation_txt  ,
remark_txt  ,
reference1_txt  ,
reference2_txt  ,
reference3_txt  

from old_fct 
)

Select 
    *
from converted_fct
   