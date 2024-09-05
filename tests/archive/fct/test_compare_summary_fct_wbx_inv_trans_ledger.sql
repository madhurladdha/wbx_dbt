{{ config( 

  enabled=false, 
  snowflake_warehouse= "EI_MEDIUM",
  severity = 'warn', 
  warn_if = '>0' 

) }} 

/* A specific (hard-coded) summary test for Weetabix Inventory Trans Ledger.
   The intent is to confirm high level balances since there are known nuances in the key fields such as line number that make 
   perfect matching impossible.
*/


select 'dbt' as flag, document_number
,sum(transaction_qty) sum_trans_qty,sum(BASE_AMT) as sum_base_amt ,sum(transaction_amt) as transaction_amt
from {{ ref('fct_wbx_inv_trans_ledger') }}
group by document_number
minus
select 'dbt' as flag, document_number
,sum(transaction_qty) sum_trans_qty,sum(BASE_AMT) as sum_base_amt ,sum(transaction_amt) as transaction_amt
from {{ ref('conv_inv_wtx_trans_ledger_fact') }}
group by document_number
union
select 'iics' as flag, document_number
,sum(transaction_qty) sum_trans_qty,sum(BASE_AMT) as sum_base_amt ,sum(transaction_amt) as transaction_amt
from {{ ref('conv_inv_wtx_trans_ledger_fact') }}
group by document_number
minus
select 'iics' as flag, document_number
,sum(transaction_qty) sum_trans_qty,sum(BASE_AMT) as sum_base_amt ,sum(transaction_amt) as transaction_amt
from {{ ref('fct_wbx_inv_trans_ledger') }}
group by document_number
order by document_number