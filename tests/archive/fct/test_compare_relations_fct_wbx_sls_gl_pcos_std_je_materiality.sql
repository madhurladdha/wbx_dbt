{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

select FISCAL_YEAR_PERIOD_NO,  TRADE_TYPE,value_type,"'IICS'" IICS,"'DBT'" DBT, "'IICS'"-"'DBT'" difference, case when "'IICS'" <>0 then abs((("'IICS'"-"'DBT'")/"'IICS'")*100) else 0 end "difference_perc%" from (
select * from (
select * from (
select   
'IICS' SOURCE_SYSTEM,
FISCAL_YEAR_PERIOD_NO,
  TRADE_TYPE,
sum(to_number(BASE_LEDGER_AMT,20,2)) BASE_LEDGER_AMT,
sum(to_number(TXN_LEDGER_AMT,20,2)) TXN_LEDGER_AMT,
sum(to_number(PHI_LEDGER_AMT,20,2)) PHI_LEDGER_AMT,
sum(to_number(PCOMP_LEDGER_AMT,20,2)) PCOMP_LEDGER_AMT

from  {{ref('conv_sls_wtx_gl_pcos_std_je_fact')}}
  group by  FISCAL_YEAR_PERIOD_NO,TRADE_TYPE

union all

select  
'DBT' SOURCE_SYSTEM,
FISCAL_YEAR_PERIOD_NO,
    TRADE_TYPE,
sum(to_number(BASE_LEDGER_AMT,20,2)) BASE_LEDGER_AMT,
sum(to_number(TXN_LEDGER_AMT,20,2)) TXN_LEDGER_AMT,
sum(to_number(PHI_LEDGER_AMT,20,2)) PHI_LEDGER_AMT,
sum(to_number(PCOMP_LEDGER_AMT,20,2)) PCOMP_LEDGER_AMT
  
from {{ref('fct_wbx_sls_gl_pcos_std_je')}}
  group by  FISCAL_YEAR_PERIOD_NO, TRADE_TYPE
 ) 
  unpivot(value_of for value_type in 
          (
BASE_LEDGER_AMT,
TXN_LEDGER_AMT,
PHI_LEDGER_AMT,
PCOMP_LEDGER_AMT

)
)
 --where nvl(FISCAL_YEAR_PERIOD_NO,0) > 2018   
)
 pivot(sum(value_of) for source_system in ('IICS', 'DBT')) 
      as p
)
--where "difference_perc%" > .1
order by 1,2,3