with paymterm as(
select * from {{ref('src_paymterm')}}

),

Final as(
    SELECT DISTINCT '{{env_var("DBT_SOURCE_SYSTEM")}}'  as SOURCE_SYSTEM,
            PAYMTERMID  as source_payment_terms_code,
            PAYMTERMID  as payment_terms_code,
            PAYMTERMID  as payment_terms_description,
            numofdays   as days_to_pay,
            null        as days_to_discount,
            null        as discount_percent,
            ROW_NUMBER() OVER(PARTITION BY PAYMTERMID order by days_to_pay desc) rn
           from paymterm
)

        /*we are recieving 2 records for same "PAYMTERM_ID" from Source .
        one from Ibe and rfl each having different "num_of_days".
        This is causing DML issue and model "XREF_WBX_PAYMENT_TERMS" was failing with duplicate issue
        Adding partition by clause to pick only one record with higher numofdays */


select * from FINAL where rn=1