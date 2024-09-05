{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

select FISCAL_YEAR_PERIOD_NO,  TRADE_TYPE,value_type,"'PROD'" PROD,"'TEST'" TEST, "'PROD'"-"'TEST'" difference, case when "'PROD'" <>0 then abs((("'PROD'"-"'TEST'")/"'PROD'")*100) else 0 end "difference_perc%" from (
select * from (
select * from (
select   
'PROD' SOURCE_SYSTEM,
FISCAL_YEAR_PERIOD_NO,
  TRADE_TYPE,
sum(to_number(BASE_LEDGER_AMT,20,2)) BASE_LEDGER_AMT,
sum(to_number(TXN_LEDGER_AMT,20,2)) TXN_LEDGER_AMT,
sum(to_number(PHI_LEDGER_AMT,20,2)) PHI_LEDGER_AMT,
sum(to_number(PCOMP_LEDGER_AMT,20,2)) PCOMP_LEDGER_AMT

from  wbx_prod.fact.fct_wbx_sls_gl_trade where document_company='WBX'
  group by  FISCAL_YEAR_PERIOD_NO,TRADE_TYPE

union all

select  
'TEST' SOURCE_SYSTEM,
FISCAL_YEAR_PERIOD_NO,
    TRADE_TYPE,
sum(to_number(BASE_LEDGER_AMT,20,2)) BASE_LEDGER_AMT,
sum(to_number(TXN_LEDGER_AMT,20,2)) TXN_LEDGER_AMT,
sum(to_number(PHI_LEDGER_AMT,20,2)) PHI_LEDGER_AMT,
sum(to_number(PCOMP_LEDGER_AMT,20,2)) PCOMP_LEDGER_AMT
  
from {{ref('fct_wbx_sls_gl_trade')}}  where document_company='WBX'
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
 pivot(sum(value_of) for source_system in ('PROD', 'TEST')) 
      as p
)
--where "difference_perc%" > .1
order by 1,2,3