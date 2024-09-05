{{
    config(
        on_schema_change="sync_all_columns",
        tags=["finance","gl","gl_trans"],
        snowflake_warehouse=env_var('DBT_WBX_SF_WH')
    )
}}

with source as (
       select * from {{ ref('stg_f_wbx_fin_gl_trans')}}
),

lkp_account as (
      select
            upper(source_system)        as source_system,
            account_guid                as account_guid,
            source_object_id            as source_object_id,
            source_company_code         as source_company_code,
            source_subsidiary_id        as source_subsidiary_id,
            source_account_identifier   as source_account_identifier,
            tagetik_account             as tagetik_account,
            consolidation_account       as consolidation_account,
            source_business_unit_code   as source_business_unit_code,
            account_category            as account_category,
            account_type                as account_type
        from {{ ref("dim_wbx_account") }}
),

dim_date as (
        select fiscal_year_period_no,fiscal_year,calendar_date,calendar_date_id from(
        select fiscal_year_period_no,fiscal_year,calendar_date_id,calendar_date,row_number()
        over(partition by fiscal_year_period_no,fiscal_year order by calendar_date ) as rno
        from {{ ref('src_dim_date')}})
),

company_master as (
        select * from {{ ref('dim_wbx_company')}}
),

ref_effective_currency_dim as (
        select 
            source_system,
            company_default_currency_code,
            effective_date,
            expiration_date,
            parent_currency_code,
            {{ dbt_utils.surrogate_key([
           'source_system',
           'company_code',
           "'COMPANY'"]) }} as company_address_guid
    from {{ref('src_ref_effective_currency_dim')}}
),
pcomp as ( 
        select  
            distinct  ltrim(rtrim(b.parent_currency_code)) as parent_currency_code,  
            ltrim(rtrim(a.source_system)) as source_system, 
            ltrim(rtrim(a.company_code)) as company_code,
            b.effective_date as effective_date,
            b.expiration_date as expiration_date 
    from company_master a
        , ref_effective_currency_dim b
        where a.company_address_guid = b.company_address_guid 
        and a.source_system= '{{env_var("DBT_SOURCE_SYSTEM")}}'
        order by source_system,company_code,effective_date,expiration_date,parent_currency_code
),
merge_tfm as (
        select 
            src.source_system	                                                as source_system,
            ltrim(rtrim(src.document_company))                                  as document_company,
            cast(substring(src.source_document_type,1,20) as text(20))          as source_document_type,
            nvl(to_char(document_type_lkp.normalized_value), 
                to_char(src.source_document_type))                              as document_type,
            cast(substring(src.document_number,1,20) as text(20))               as document_number,
            cast(substring(src.source_account_identifier,1,20) as text(20))     as source_account_identifier,
            src.gl_date	                                                        as gl_date,
            cast(src.journal_line_number as number(38,10))                      as journal_line_number,
            cast(substring(src.gl_extension_code,1,2) as text(2))               as gl_extension_code,
            src.source_business_unit_code	                                    as source_business_unit_code,
            null                                                                as source_subledger_identifier, 
	        null                                                                as source_subledger_type, 
            src.void_flag	                                                    as void_flag,
            case when src.payment_trans_date is null then
            try_to_date('9999-12-31','yyyy-mm-dd') 
            else src.payment_trans_date end                                     as payment_trans_date,
            src.document_pay_item	                                            as document_pay_item,
            src.payment_number	                                                as payment_number,
            src.payment_identifier	                                            as payment_identifier,
            lkp_account.consolidation_account                                   as target_account_identifier,
            src.company_code || '.' || src.source_business_unit_code
            || '.'|| src.source_object_code                                 
                                                                                as source_concat_nat_key,
            {{
                dbt_utils.surrogate_key(
                    ["src.source_system", "source_concat_nat_key"]
                )
            }}                                                                  as account_guid,
            src.source_ledger_type	                                            as source_ledger_type,
            nvl(to_char(ledger_type_lkp.normalized_value), 
                to_char(src.source_ledger_type))                                as ledger_type,
            src.gl_posted_flag	                                                as gl_posted_flag,
            src.batch_number                                                    as batch_number,
            src.batch_type	                                                    as batch_type,
            src.batch_date	                                                    as batch_date,
            source_address_number	                                            as source_address_number,
            {{
                dbt_utils.surrogate_key(
                 ["src.source_system",
                "src.source_address_number",]
                )
            }}                                                                  as address_guid,
            lkp_account.source_company_code                                     as company_code,
            lkp_account.source_object_id	                                    as source_object_id,
            lkp_account.source_subsidiary_id	                                as source_subsidiary_id,
            ltrim(rtrim(src.company_code))                                      as source_company_code,
            case when (src.source_business_unit_code) = '-' then '0'
             else
            {{
                dbt_utils.surrogate_key(
                [
                    "src.source_system",
                    "src.source_business_unit_code",
                    "'PLANT_DC'",
                ]
                )
            }} end                                                              as business_unit_address_guid,
            0                                                                   as subledger_guid, 
	        null                                                                as subledger_type_desc, 
            case when regexp_instr(lkp_account.consolidation_account,'2')=1  
            or regexp_instr(lkp_account.consolidation_account,'4')=1 then 
            src.txn_ledger_amt*-1 else src.txn_ledger_amt end                   as txn_ledger_amt,
            nvl(to_char(transaction_currency_lkp.normalized_value), 
                to_char(src.transaction_currency))                              as transaction_currency,
            case when regexp_instr(lkp_account.consolidation_account ,'2')=1  
            or regexp_instr(lkp_account.consolidation_account ,'4')=1 then
            src.base_ledger_amt*-1 else src.base_ledger_amt end                 as base_ledger_amt,	
            src.txn_quantity	                                                as quantity,
            src.supplier_invoice_number	                                        as supplier_invoice_number,
            src.invoice_date	                                                as invoice_date,
            src.account_cat21_code	                                            as account_cat21_code,
            src.source_date_updated                                             as source_date_updated,
          --  concat(to_date(src.source_date_updated)||' '||'00:00:00.000')       as source_date_updated,
            current_timestamp()	                                                as load_date,
            current_timestamp()	                                                as update_date,
            dim_dt.calendar_date_id	                                            as source_updated_d_id,
            src.explanation_txt	                                                as explanation_txt,
            src.remark_txt	                                                    as remark_txt,
            src.reference1_txt	                                                as reference1_txt,
            src.reference2_txt	                                                as reference2_txt,
            src.reference3_txt	                                                as reference3_txt,
            src.transaction_uom	                                                as transaction_uom,
            src.department	                                                    as department,
            src.caf_no	                                                        as caf_no,
            src.plant	                                                        as plant,
            src.product_dim	                                                    as product_dim,
            src.customer_dim	                                                as customer_dim,
            src.txn_ledger_amt                                                  as oc_txn_ledger_amt,
            src.base_ledger_amt                                                 as oc_base_ledger_amt,
            src.journal_category	                                            as journal_category,
            case when curr_exch.curr_conv_rt is null  then 0 else 
            curr_exch.curr_conv_rt end                                          as v_phi_conv_rt,
            src.base_currency                                                   as base_currency,
            curr_exch.curr_conv_rt                                              as curr_conv_rt,
            case  when base_ledger_amt = 0  then 0 
            else (txn_ledger_amt / base_ledger_amt) end                         as v_txn_conv_rt,
            pcomp.parent_currency_code                                          as v_pcomp_currency,
            src.transledger_journal_number                                      as transledger_journal_number,
            src.ledger_account                                                  as ledger_account,
            src.trade_type                                                      as trade_type,
            src.sku                                                             as sku,
            src.Promo_Term_Code                                                 as Promo_Term_Code,
            src.main_account                                                    as main_account,
            src.main_account_name                                               as main_account_name, 
            src.cost_center                                                     as cost_center,
            src.cost_center_name                                                as cost_center_name,
            src.product_class                                                   as product_class,
            src.product_class_name                                              as product_class_name,
            src.us_account                                                      as us_account,
            src.us_account_name                                                 as us_account_name,
            lkp_account.account_category                                        as account_category,
            lkp_account.account_type                                            as account_type
        from source src
            left outer join lkp_account on lkp_account.source_system = src.source_system 
                and lkp_account.source_account_identifier = src.source_account_identifier            
                and lkp_account.source_company_code=src.company_code
                and lkp_account.source_business_unit_code=src.source_business_unit_code
            left outer join --dim_date dim_dt on src.gl_date=dim_dt.calendar_date
                dim_date dim_dt on dim_dt.calendar_date = to_char(src.source_date_updated, 'yyyy-mm-dd')
            left outer join
            {{
                ent_dbt_package.lkp_exchange_rate_daily(
                    "base_currency", "'USD'", "gl_date", "curr_exch"
                )
            }}
            left outer join pcomp on src.source_system=pcomp.source_system and 
            src.company_code=pcomp.company_code and src.gl_date>=pcomp.effective_date and 
            src.gl_date<=pcomp.expiration_date
                
            left outer join
                {{ent_dbt_package.lkp_normalization("UPPER(ltrim(rtrim(src.source_system)))",
                "FINANCE","DOCUMENT_TYPE_CODE",
                "UPPER(ltrim(rtrim(src.source_document_type)))","document_type_lkp")}}
            left outer join
                {{ent_dbt_package.lkp_normalization("src.source_system","ADDRESS_BOOK","CUST_CURRENCY_CODE",
                "UPPER(ltrim(rtrim(src.transaction_currency)))","transaction_currency_lkp")}}
            left outer join 
                {{ent_dbt_package.lkp_normalization("src.source_system","FINANCE","LEDGER_TYPE_CODE",
                "UPPER(ltrim(rtrim(src.source_ledger_type)))","ledger_type_lkp")}}
),
exp_rt_calc as (
            select 
                cast(substring(source_system,1,255) as text(255))                as source_system,
                cast(substring(tf.document_company,1,20) as text(20) )           as document_company,
                cast(substring(tf.source_document_type,1,20) as text(20))        as source_document_type,
                cast(substring(tf.document_type,1,20) as text(20))               as document_type,
                cast(substring(tf.document_number,1,255) as text(255))           as document_number,
                cast(substring(tf.source_account_identifier,1,255) as text(255)) as source_account_identifier,
                cast(tf.gl_date as timestamp_ntz(9))	                         as gl_date,
                cast(tf.journal_line_number as number(38,0) )	                 as journal_line_number,
                cast(substring(tf.gl_extension_code,1,2) as text(2)) 	         as gl_extension_code,
                tf.source_business_unit_code	                                 as source_business_unit_code,
                tf.source_subledger_identifier                                   as source_subledger_identifier, 
	            tf.source_subledger_type                                         as source_subledger_type, 
                tf.void_flag	                                                 as void_flag,
                tf.payment_trans_date                                            as payment_trans_date,
                tf.document_pay_item	                                         as document_pay_item,
                tf.payment_number	                                             as payment_number,
                tf.payment_identifier	                                         as payment_identifier,
                tf.target_account_identifier                                     as target_account_identifier,
                tf.account_guid                                                  as account_guid,
                tf.source_ledger_type	                                         as source_ledger_type,
                tf.ledger_type                                                   as ledger_type,
                tf.gl_posted_flag	                                             as gl_posted_flag,
                tf.batch_number                                                  as batch_number,
                tf.batch_type	                                                 as batch_type,
                tf.batch_date	                                                 as batch_date,
                tf.source_address_number	                                     as source_address_number,
                tf.address_guid                                                  as address_guid,
                tf.company_code	                                                 as company_code,
                tf.source_object_id	                                             as source_object_id,
                tf.source_subsidiary_id	                                         as source_subsidiary_id,
                tf.source_company_code                                           as source_company_code,
                tf.business_unit_address_guid                                     as business_unit_address_guid,
                tf.subledger_guid                                                as subledger_guid, 
	            tf.subledger_guid                                                as subledger_type_desc,
                tf.txn_ledger_amt	                                             as txn_ledger_amt,
                tf.transaction_currency                                          as transaction_currency,
                tf.base_ledger_amt	                                             as base_ledger_amt,
                tf.quantity	                                                     as quantity,
                tf.supplier_invoice_number	                                     as supplier_invoice_number,
                tf.invoice_date	                                                 as invoice_date,
                tf.account_cat21_code	                                         as account_cat21_code,
                tf.source_date_updated	                                         as source_date_updated, 
                current_timestamp()	                                             as load_date,
                current_timestamp()	                                             as update_date,
                tf.source_updated_d_id	                                         as source_updated_d_id,
                tf.explanation_txt	                                             as explanation_txt,
                tf.remark_txt	                                                 as remark_txt,
                tf.reference1_txt	                                             as reference1_txt,
                tf.reference2_txt	                                             as reference2_txt,
                tf.reference3_txt	                                             as reference3_txt,
                tf.transaction_uom	                                             as transaction_uom,
                tf.department	                                                 as department,
                tf.caf_no	                                                     as caf_no,
                tf.plant	                                                     as plant,
                tf.product_dim	                                                 as product_dim,
                tf.customer_dim	                                                 as customer_dim,
                tf.journal_category	                                             as journal_category,
                tf.oc_txn_ledger_amt                                             as oc_txn_ledger_amt,
                tf.oc_base_ledger_amt                                            as oc_base_ledger_amt,
                tf.v_phi_conv_rt                                                 as v_phi_conv_rt,
                tf.base_currency                                                 as base_currency,
                tf.curr_conv_rt                                                  as curr_conv_rt,
                tf.v_txn_conv_rt                                                 as v_txn_conv_rt,
                tf.v_pcomp_currency                                              as v_pcomp_currency,
                case when base_currency = v_pcomp_currency 
                then 1 else case when curr_exch_pcom.curr_conv_rt 
                is null then 0 else curr_exch_pcom.curr_conv_rt
                end end                                                          as v_pcomp_conv_rt,  
                round(v_phi_conv_rt * base_ledger_amt,2)                         as phi_ledger_amt,
                tf.transledger_journal_number                                   as transledger_journal_number,
                tf.ledger_account                                               as ledger_account,
                tf.trade_type                                                   as trade_type,
                tf.sku                                                          as sku,
                tf.Promo_Term_Code                                              as Promo_Term_Code,
                tf.main_account                                                 as main_account,
                tf.main_account_name                                            as main_account_name, 
                tf.cost_center                                                  as cost_center,
                tf.cost_center_name                                             as cost_center_name,
                tf.product_class                                                as product_class,
                tf.product_class_name                                           as product_class_name,
                tf.us_account                                                   as us_account,
                tf.us_account_name                                              as us_account_name,
                cast(substring(tf.account_category,1,255) as text(255))         as account_category,
                cast(substring(tf.account_type,1,255) as text(255))             as account_type       
            from merge_tfm tf
              left outer join
            {{
                ent_dbt_package.lkp_exchange_rate_daily(
                    "base_currency", "v_pcomp_currency", "gl_date", "curr_exch_pcom"
                )
            }}

),

final as (
    select 
        
        {{
            dbt_utils.surrogate_key(
                [
                    "ex.source_system",
                    "ex.document_company",
                    "ex.source_document_type",
                    "ex.document_type",
                    "ex.document_number",
                    "ex.source_account_identifier",
                    "ex.gl_date",
                    "ex.journal_line_number",
                    "ex.gl_extension_code",
                ]
            )
        }}                                                      as unique_key,
        ex.source_system	                                    as source_system,
        ex.document_company                                     as document_company,
        ex.source_document_type	                                as source_document_type,
        ex.document_type                                        as document_type,
        ex.document_number                                      as document_number,
        ex.source_account_identifier	                        as source_account_identifier,
        ex.gl_date	                                            as gl_date,
        ex.journal_line_number	                                as journal_line_number,
        ex.gl_extension_code	                                as gl_extension_code,
        ex.source_business_unit_code	                        as source_business_unit_code,
        ex.source_subledger_identifier                          as source_subledger_identifier, 
	    ex.source_subledger_type                                as source_subledger_type, 
        ex.void_flag	                                        as void_flag,
        ex.payment_trans_date                                   as payment_trans_date,
        ex.document_pay_item	                                as document_pay_item,
        ex.payment_number	                                    as payment_number,
        ex.payment_identifier	                                as payment_identifier,
        ex.target_account_identifier                            as target_account_identifier,
        ex.account_guid                                         as account_guid,
        ex.source_ledger_type	                                as source_ledger_type,
        ex.ledger_type                                          as ledger_type,
        ex.gl_posted_flag	                                    as gl_posted_flag,
        ex.batch_number                                         as batch_number,
        ex.batch_type	                                        as batch_type,
        ex.batch_date	                                        as batch_date,
        ex.source_address_number	                            as source_address_number,
        ex.address_guid                                         as address_guid,
        ex.company_code	                                        as company_code,
        ex.source_object_id	                                    as source_object_id,
        ex.source_subsidiary_id	                                as source_subsidiary_id,
        ex.source_company_code                                  as source_company_code,
        ex.business_unit_address_guid                           as business_unit_address_guid,
        ex.subledger_guid                                       as subledger_guid, 
	    ex.subledger_guid                                       as subledger_type_desc,
        ex.txn_ledger_amt	                                    as txn_ledger_amt,
        ex.transaction_currency                                 as transaction_currency,
        ex.base_ledger_amt	                                    as base_ledger_amt,
        ex.quantity	                                            as quantity,
        ex.supplier_invoice_number	                            as supplier_invoice_number,
        ex.invoice_date	                                        as invoice_date,
        ex.account_cat21_code	                                as account_cat21_code,
        ex.source_date_updated	                                as source_date_updated,
        ex.load_date                                            as load_date,
        ex.update_date                                          as update_date,
        ex.source_updated_d_id	                                as source_updated_d_id,
        ex.explanation_txt	                                    as explanation_txt,
        ex.remark_txt	                                        as remark_txt,
        ex.reference1_txt	                                    as reference1_txt,
        ex.reference2_txt	                                    as reference2_txt,
        ex.reference3_txt	                                    as reference3_txt,
        ex.transaction_uom	                                    as transaction_uom,
        ex.department	                                        as department,
        ex.caf_no	                                            as caf_no,
        ex.plant	                                            as plant,
        ex.product_dim	                                        as product_dim,
        ex.customer_dim	                                        as customer_dim,
        ex.journal_category	                                    as journal_category,
        ex.oc_txn_ledger_amt                                    as oc_txn_ledger_amt,
        ex.oc_base_ledger_amt                                   as oc_base_ledger_amt,
        ex.base_currency                                        as base_currency,
        ex.curr_conv_rt                                         as curr_conv_rt,
        ex.phi_ledger_amt                                       as phi_ledger_amt,
        round(v_pcomp_conv_rt * base_ledger_amt,2)              as pcomp_ledger_amt,
        ex.v_txn_conv_rt                                        as txn_conv_rt,
        'USD'                                                   as phi_currency,
        ex.v_phi_conv_rt                                        as phi_conv_rt , 
        ex.v_pcomp_currency                                     as pcomp_currency,
        ex.v_pcomp_conv_rt                                      as pcomp_conv_rt,
        round(v_phi_conv_rt * oc_base_ledger_amt,2)             as oc_phi_ledger_amt,
        round(v_pcomp_conv_rt * oc_base_ledger_amt,2)           as oc_pcomp_ledger_amt,
        ex.transledger_journal_number                           as transledger_journal_number,
        ex.ledger_account                                       as ledger_account,
        ex.trade_type                                           as trade_type,
        ex.sku                                                  as sku,
        ex.Promo_Term_Code                                      as Promo_Term_Code,
        ex.main_account                                         as main_account,
        ex.main_account_name                                    as main_account_name, 
        ex.cost_center                                          as cost_center,
        ex.cost_center_name                                     as cost_center_name,
        ex.product_class                                        as product_class,
        ex.product_class_name                                   as product_class_name,
        ex.us_account                                           as us_account,
        ex.us_account_name                                      as us_account_name  ,
        ex.account_category                                     as account_category,
        ex.account_type                                         as account_type
    
    from exp_rt_calc ex    
)
select * from final
