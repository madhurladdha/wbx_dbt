{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 


 

select * from 
        (
                select Transacion_idenfier,value_type,"'SRC'" SRC,"'FACT'" FACT, "'SRC'"-"'FACT'" difference,
                case when "'SRC'" <>0 then abs((("'SRC'"-"'FACT'")/"'SRC'")*100) else 0 end difference_percent
                from (
                        select * from (
                                    select * from (
                                    select   
                                    'SRC' SOURCE_SYSTEM,
                                    salesid transacion_idenfier,
                                    to_number(qtyordered,20,2) quantity_ordered,
                                    to_number(lineamount,20,2) line_amount 
                                    from   "WBX_DEV"."ZZ_RRAJAGOPALAN"."SRC_SALESLINE" where upper(DATAAREAID) not in ('WBX')

                                    union all

                                    select   
                                    'FACT' SOURCE_SYSTEM,
                                    sales_order_number transacion_idenfier,
                                    to_number(ORDERED_TRAN_QUANTITY,20,2) quantity_ordered,
                                    to_number(TRANS_RPT_GRS_AMT,20,2) line_amount     
                                    from   WBX_DEV.ZZ_RRAJAGOPALAN.fct_wbx_sls_order  where NVL(SALES_ORDER_COMPANY,'IBE')  in ('RFL','IBE')
                        ) 
                    unpivot(value_of for value_type in 
                                (
                                quantity_ordered,
                                line_amount
                                )
                            )
                )
                pivot(sum(value_of) for source_system in ('SRC', 'FACT')) 
                    as p
        )
)
where difference_percent <> 0
order by TRANSACION_IDENFIER,VALUE_TYPE