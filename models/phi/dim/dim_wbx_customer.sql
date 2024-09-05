{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns',
    tags="rdm_core"
    )
}}

/*union logic with history has been shifted to immidiate upstream model as it was required for address model in enterprise*/

WITH int as (
    select  * from {{ref('int_d_wbx_customer')}}
    ), 

Final_Dim as (
        select  
        cast(substr(unique_key                         ,1,255)  as text(255))      as unique_key,
        cast(substr(customer_address_number_guid       ,1,255)  as text(255))      as customer_address_number_guid,
        CAST(CUSTOMER_ADDRESS_NUMBER_GUID_OLD AS NUMBER(38,0))  as CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
        cast(substr(company_address_guid               ,1,255)  as text(255))      as company_address_guid,
        CAST(COMPANY_ADDRESS_GUID_OLD AS NUMBER(38,0)) AS COMPANY_ADDRESS_GUID_OLD,
        cast(substr(GENERIC_ADDRESS_TYPE               ,1,255)  as text(255))      as GENERIC_ADDRESS_TYPE,
        cast(substr(source_system                      ,1,255)  as text(255))      as source_system,
        cast(substr(source_system_address_number       ,1,255)  as text(255))      as source_system_address_number,
        cast(substr(company_code                       ,1,255)  as text(255))      as company_code,
        cast(substr(company_name                       ,1,255)  as text(255))      as company_name,
        cast(substr(customer_name                      ,1,255)  as text(255))      as customer_name,
        cast(substr(customer_type                      ,1,255)  as text(255))      as customer_type,
        cast(substr(bill_to                            ,1,255)  as text(255))      as bill_to,
        cast(substr(bill_address_type                  ,1,255)  as text(255))      as bill_address_type,
        cast(substr(bill_address_type_description      ,1,255)  as text(255))      as bill_address_type_description,
        cast(substr(freight_handling_code              ,1,255)  as text(255))      as freight_handling_code,
        cast(substr(payment_terms_code                 ,1,255)  as text(255))      as payment_terms_code,
        cast(substr(payment_terms_description          ,1,255)  as text(255))      as payment_terms_description,
        cast(substr(csr_address_number                 ,1,255)  as text(255))      as csr_address_number,
        cast(substr(csr_name                           ,1,255)  as text(255))      as csr_name,
        cast(substr(credit_limit                       ,1,255)  as text(255))      as credit_limit,
        cast(substr(unified_customer                   ,1,255)  as text(255))      as unified_customer,
        cast(substr(shipping_terms                     ,1,255)  as text(255))      as shipping_terms,
        cast(substr(source_customer_code               ,1,255)  as text(255))      as source_customer_code,
        cast(substr(customer_group                     ,1,255)  as text(255))      as customer_group,
        cast(substr(customer_group_name                ,1,255)  as text(255))      as customer_group_name,
        cast(substr(legacy_customer_number             ,1,255)  as text(255))      as legacy_customer_number,
        cast(substr(currency_code                      ,1,255)  as text(255))      as currency_code,
        cast(substr(transport_mode                     ,1,255)  as text(255))      as transport_mode,
        cast(substr(bill_name                          ,1,255)  as text(255))      as bill_name,
        cast(substr(date_inserted                      ,1,255)  as text(255))      as date_inserted,
        cast(substr(date_updated                       ,1,255)  as text(255))      as date_updated,
        NULL                                                                       as customer_price_group,
        NULL                                                                       as channel,
        NULL                                                                       as invoice_method,
        cast(substr(conv_status                       ,1,50)  as text(50))         as CONV_STATUS
        
     from int
)

Select * from Final_Dim 