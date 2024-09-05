{{ config( 

  enabled=true, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


/*
This test is to reconcile ledger amount before and after allocation for GL DNI.
This also provides list of all the records from source which didn't get allocated.

*/

{% set account_category_list = "('PCOS STD - BOUGHT IN','PCOS STD - OTHER','PCOS STD - FG','PCOS STD - INTERCO')" %}

with gl_source as (
    select
        source_object_id,
        gl_date,
        document_company,
        source_account_identifier,
        sum(oc_txn_ledger_amt) as txn_ledger_amt,
        sum(oc_base_ledger_amt) as base_ledger_amt,
        sum(oc_phi_ledger_amt) as phi_ledger_amt,
        sum(oc_pcomp_ledger_amt) as pcomp_ledger_amt
    from {{ ref('fct_wbx_fin_gl_trans') }}
    where upper(trim(account_category)) in {{account_category_list}} and 
        upper(document_company) in {{env_var('DBT_D365_COMPANY_FILTER')}} 
    group by all
),

gl_trade as (
    select
        source_object_id,
        gl_date,
        document_company,
        source_account_identifier,
        union_logic,
        sum(txn_ledger_amt) as txn_ledger_amt,
        sum(base_ledger_amt) as base_ledger_amt,
        sum(phi_ledger_amt) as phi_ledger_amt,
        sum(pcomp_ledger_amt) as pcomp_ledger_amt
    from {{ ref('fct_wbx_sls_gl_pcos_std_je') }}
    group by all
),

var_check as (
    select
        gl.source_object_id,
        gl.gl_date,
        gl.document_company,
        gl.source_account_identifier,
        trd.union_logic,
        abs(gl.txn_ledger_amt - trd.txn_ledger_amt) as var_txn_ledger_amt,
        abs(gl.base_ledger_amt - trd.base_ledger_amt) as var_base_ledger_amt,
        abs(gl.phi_ledger_amt - trd.phi_ledger_amt) as var_phi_ledger_amt,
        abs(gl.pcomp_ledger_amt - trd.pcomp_ledger_amt) as var_pcomp_ledger_amt
    from gl_source as gl left join gl_trade as trd
        on
            gl.source_object_id = trd.source_object_id
            and gl.gl_date = trd.gl_date
            and gl.document_company = trd.document_company
            and gl.source_account_identifier = trd.source_account_identifier
)

select * from var_check
where
    var_txn_ledger_amt > 0.01 or var_base_ledger_amt > 0.01
    or var_phi_ledger_amt > 0.01 or var_pcomp_ledger_amt > 0.01 or union_logic is null