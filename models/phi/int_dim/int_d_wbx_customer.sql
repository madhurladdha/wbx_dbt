with stg_customers AS (
select * from {{ref('stg_d_wbx_customer')}}

),

hist_customers as (
    select * from {{ref('conv_dim_wbx_customer')}}
),

all_cust_v3 AS (
SELECT 
        CAST (SOURCE_SYSTEM AS VARCHAR2 (255))                  AS 	SOURCE_SYSTEM,
        CAST (SOURCE_SYSTEM_ADDRESS_NUMBER AS VARCHAR2 (255))   AS 	SOURCE_SYSTEM_ADDRESS_NUMBER,
        'CUSTOMER_MAIN'                                              AS  GENERIC_ADDRESS_TYPE,
        'COMPANY'                                               AS  COMPANY_GENERIC_ADDRESS_TYPE,
        CAST (SOURCE_NAME AS VARCHAR2 (255))                    AS 	SOURCE_NAME,
        CAST (ADDRESS_TYPE AS VARCHAR2 (255))                   AS 	ADDRESS_TYPE,
        CAST (ADDRESS_TYPE_DESCRIPTION AS VARCHAR2 (255))       AS 	ADDRESS_TYPE_DESCRIPTION,
        CAST (ADDRESS_LINE_1 AS VARCHAR2 (255))                 AS 	ADDRESS_LINE_1,
        CAST (CITY AS VARCHAR2 (255))                           AS 	CITY,
        CAST (STATE_PROVINCE_CODE AS VARCHAR2 (255))            AS 	STATE_PROVINCE_CODE,
        CAST (STATE_PROVINCE AS VARCHAR2 (255))                 AS 	STATE_PROVINCE,
        CAST (POSTAL_CODE AS VARCHAR2 (255))                    AS 	POSTAL_CODE,
        CAST (COUNTRY_CODE AS VARCHAR2 (255))                   AS 	COUNTRY_CODE,
        CAST (COUNTRY AS VARCHAR2 (255))                        AS 	COUNTRY,
        CAST (ACTIVE_INDICATOR AS VARCHAR2 (255))               AS 	ACTIVE_INDICATOR,
        CAST (TAX_NUMBER as VARCHAR2 (255))                     AS  TAX_NUMBER,
        CAST (LONG_ADDRESS_NUMBER AS VARCHAR2 (255))            AS 	LONG_ADDRESS_NUMBER,
       CAST (coalesce(trim(PLAN_COMPANY), '-') AS VARCHAR2 (255))  AS 	COMPANY_CODE,
        CAST (PLAN_COMPANY_NAME AS VARCHAR2 (255))              AS 	COMPANY_NAME,
        CAST (CUSTOMER_NAME AS VARCHAR2 (255))                  AS 	CUSTOMER_NAME,
        CAST (CUSTOMER_TYPE AS VARCHAR2 (255))                  AS 	CUSTOMER_TYPE,
        CAST(BILL_TO     AS VARCHAR2 (255))                     AS 	BILL_TO,
        CAST (CUST_SHIPPING_TERMS AS VARCHAR2 (255))            AS 	SHIPPING_TERMS,
        CAST (CUST_PAYMENT_TERMS_CODE AS VARCHAR2 (255))        AS 	PAYMENT_TERMS_CODE,
        CAST (CUST_PAYMENT_TERMS_DESC AS VARCHAR2 (255))        AS 	PAYMENT_TERMS_DESCRIPTION,
        CAST (CSR_ADDRESS_NUMBER AS VARCHAR2 (255))             AS 	CSR_ADDRESS_NUMBER,
        CAST (CSR_NAME AS VARCHAR2 (255))                       AS 	CSR_NAME,
        CAST (CREDIT_LIMIT AS VARCHAR2 (255))                   AS  CREDIT_LIMIT,
        CAST (CUST_CURRENCY_CODE AS VARCHAR2 (255))             AS  CURRENCY_CODE,
        CAST (CUST_TRANSPORT_MODE AS VARCHAR2 (255))            AS  TRANSPORT_MODE,
        CAST (NULL AS VARCHAR2 (255))                           AS 	SOURCE_CUSTOMER_CODE,
        CAST (LEGACY_CUSTOMER_NUMBER AS VARCHAR2 (255))         AS 	LEGACY_CUSTOMER_NUMBER,
        CAST (CUSTOMER_GROUP AS VARCHAR2 (255))                 AS 	CUSTOMER_GROUP,
        CAST (CUSTOMER_GROUP_NAME AS VARCHAR2 (255))            AS  CUSTOMER_GROUP_NAME,
        CAST (BILL_NAME AS VARCHAR2 (255))                      AS  BILL_NAME
       



FROM stg_customers
)
, normalized_customers AS (
    SELECT 
        GENERIC_ADDRESS_TYPE,
        {{ dbt_utils.surrogate_key(['src.source_system','src.source_system_address_number','GENERIC_ADDRESS_TYPE','src.company_code']) }} AS CUSTOMER_ADDRESS_NUMBER_GUID,
        NULL AS CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
        {{ dbt_utils.surrogate_key(['src.source_system','src.company_code','COMPANY_GENERIC_ADDRESS_TYPE']) }} AS COMPANY_ADDRESS_GUID,
        null AS COMPANY_ADDRESS_GUID_old,
        SRC.SOURCE_SYSTEM,
        SRC.SOURCE_SYSTEM_ADDRESS_NUMBER,
        SRC.ADDRESS_LINE_1,
        SRC.CITY,
        SRC.POSTAL_CODE,
        SRC.ACTIVE_INDICATOR,
        SRC.TAX_NUMBER,
        SRC.LONG_ADDRESS_NUMBER,
        SRC.COMPANY_CODE,
        SRC.COMPANY_NAME,
        SRC.CUSTOMER_NAME,
        SRC.CUSTOMER_TYPE,
        SRC.BILL_TO,  
        SRC.SHIPPING_TERMS  AS FREIGHT_HANDLING_CODE,
        SRC.SHIPPING_TERMS  AS SHIPPING_TERMS,
        NVL(TO_CHAR(CUST_PAYMENT_TERMS_CODE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PAYMENT_TERMS_CODE)) AS PAYMENT_TERMS_CODE,
        NVL(TO_CHAR(CUST_PAYMENT_TERMS_DESC_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PAYMENT_TERMS_DESCRIPTION)) AS PAYMENT_TERMS_DESCRIPTION,
        /* NVL(TO_CHAR(UNIFIED_CUSTOMER_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)) AS UNIFIED_CUSTOMER,
        As part of D365 change mapping UNIFIED_CUSTOMER to bill name as we are seeing issue when we have same customer number 0305421-0001
        for different company. Currently normilization table cannot store data based on company so mapping this to bill name  */
        SRC.BILL_NAME AS UNIFIED_CUSTOMER,
        NVL(TO_CHAR(COUNTRY_CODE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.COUNTRY_CODE)) AS COUNTRY_CODE,
        NVL(TO_CHAR(COUNTRY_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.COUNTRY_CODE))      AS COUNTRY,
        NVL(TO_CHAR(STATE_PROVINCE_CODE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.STATE_PROVINCE_CODE)) AS STATE_PROVINCE_CODE,
        NVL(TO_CHAR(STATE_PROVINCE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.STATE_PROVINCE_CODE))      AS STATE_PROVINCE,
        NVL(TO_CHAR(ADDRESS_TYPE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.ADDRESS_TYPE)) AS BILL_ADDRESS_TYPE,
        NVL(TO_CHAR(ADDRESS_TYPE_DESCRIPTION_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.ADDRESS_TYPE_DESCRIPTION)) AS BILL_ADDRESS_TYPE_DESCRIPTION,
        SRC.CSR_ADDRESS_NUMBER,
        SRC.CSR_NAME,
        SRC.CREDIT_LIMIT,
        TO_CHAR(systimestamp()) AS DATE_INSERTED,
        TO_CHAR(systimestamp()) AS DATE_UPDATED,
        SRC.SOURCE_CUSTOMER_CODE,
        SRC.SOURCE_NAME,
        SRC.CUSTOMER_GROUP,
        SRC.CUSTOMER_GROUP_NAME,
        SRC.LEGACY_CUSTOMER_NUMBER,
        SRC.CURRENCY_CODE,
        SRC.TRANSPORT_MODE,
        SRC.BILL_NAME

    FROM all_cust_v3 SRC
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','ADDRESS_TYPE','UPPER(SRC.ADDRESS_TYPE)','ADDRESS_TYPE_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','ADDRESS_TYPE_DESCRIPTION','UPPER(SRC.ADDRESS_TYPE_DESCRIPTION)','ADDRESS_TYPE_DESCRIPTION_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COUNTRY','UPPER(SRC.COUNTRY_CODE)','COUNTRY_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COUNTRY_CODE','UPPER(SRC.COUNTRY_CODE)','COUNTRY_CODE_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','CUST_PAYMENT_TERMS_CODE','UPPER(SRC.PAYMENT_TERMS_CODE)','CUST_PAYMENT_TERMS_CODE_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','CUST_PAYMENT_TERMS_DESC','UPPER(SRC.PAYMENT_TERMS_DESCRIPTION)','CUST_PAYMENT_TERMS_DESC_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','STATE_PROVINCE_CODE','UPPER(SRC.STATE_PROVINCE_CODE)','STATE_PROVINCE_CODE_LKP') }}
        LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','STATE_PROVINCE','UPPER(SRC.STATE_PROVINCE_CODE)','STATE_PROVINCE_LKP') }}
       /* LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','UNIFIED_CUSTOMER','UPPER(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)','UNIFIED_CUSTOMER_LKP') }} */
),


INT as (
    select 
        {{ dbt_utils.surrogate_key(['customer_address_number_guid']) }} AS UNIQUE_KEY, * from normalized_customers 
), 

new_dim as (
        select  
        cast(substr(A.unique_key                         ,1,255)  as text(255))      as unique_key,
        cast(substr(A.customer_address_number_guid       ,1,255)  as text(255))      as customer_address_number_guid,
        CAST(nvl(A.CUSTOMER_ADDRESS_NUMBER_GUID_OLD,B.CUSTOMER_ADDRESS_NUMBER_GUID_OLD) AS NUMBER(38,0))  as CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
        cast(substr(A.company_address_guid               ,1,255)  as text(255))      as company_address_guid,
        CAST(nvl(A.COMPANY_ADDRESS_GUID_OLD,B.COMPANY_ADDRESS_GUID_OLD) AS NUMBER(38,0)) AS COMPANY_ADDRESS_GUID_OLD,
        cast(substr(A.GENERIC_ADDRESS_TYPE               ,1,255)  as text(255))      as GENERIC_ADDRESS_TYPE,
        cast(substr(A.source_system                      ,1,255)  as text(255))      as source_system,
        cast(substr(A.source_system_address_number       ,1,255)  as text(255))      as source_system_address_number,
        cast(substr(A.company_code                       ,1,255)  as text(255))      as company_code,
        cast(substr(A.company_name                       ,1,255)  as text(255))      as company_name,
        cast(substr(A.customer_name                      ,1,255)  as text(255))      as customer_name,
        cast(substr(A.customer_type                      ,1,255)  as text(255))      as customer_type,
        cast(substr(A.bill_to                            ,1,255)  as text(255))      as bill_to,
        cast(substr(A.bill_address_type                  ,1,255)  as text(255))      as bill_address_type,
        cast(substr(A.bill_address_type_description      ,1,255)  as text(255))      as bill_address_type_description,
        cast(substr(A.freight_handling_code              ,1,255)  as text(255))      as freight_handling_code,
        cast(substr(A.payment_terms_code                 ,1,255)  as text(255))      as payment_terms_code,
        cast(substr(A.payment_terms_description          ,1,255)  as text(255))      as payment_terms_description,
        cast(substr(A.csr_address_number                 ,1,255)  as text(255))      as csr_address_number,
        cast(substr(A.csr_name                           ,1,255)  as text(255))      as csr_name,
        cast(substr(A.credit_limit                       ,1,255)  as text(255))      as credit_limit,
        cast(substr(A.unified_customer                   ,1,255)  as text(255))      as unified_customer,
        cast(substr(A.shipping_terms                     ,1,255)  as text(255))      as shipping_terms,
        cast(substr(A.source_customer_code               ,1,255)  as text(255))      as source_customer_code,
        cast(substr(a.customer_group                     ,1,255)  as text(255))      as customer_group,
        cast(substr(a.customer_group_name                ,1,255)  as text(255))      as customer_group_name,
        cast(substr(a.legacy_customer_number             ,1,255)  as text(255))      as legacy_customer_number,
        cast(substr(a.currency_code                      ,1,255)  as text(255))      as currency_code,
        cast(substr(a.transport_mode                     ,1,255)  as text(255))      as transport_mode,
        cast(substr(a.bill_name                          ,1,255)  as text(255))      as bill_name,
        cast(substr(a.date_inserted                      ,1,255)  as text(255))      as date_inserted,
        cast(substr(a.date_updated                       ,1,255)  as text(255))      as date_updated,
        NULL                                                                         as customer_price_group,
        NULL                                                                         as channel,
        NULL                                                                         as invoice_method,
        'N'                                                                          as CONV_STATUS,
        /*ADDRESS_FIELDS*/
       cast(substring(a.address_line_1,1,255) as text(255) )                         as address_line_1  ,
       cast(substring(a.city,1,255) as text(255) )                                   as city  ,
       cast(substring(a.state_province_code,1,255) as text(255) )                    as state_province_code  ,
       cast(substring(a.state_province,1,255) as text(255) )                         as state_province  ,
       cast(substring(a.country_code,1,255) as text(255) )                           as country_code  ,
       cast(substring(a.country,1,255) as text(255) )                                as country  ,
	   cast(substring(a.postal_code,1,255) as text(255) )                            as postal_code  ,
       cast(substring(a.LONG_ADDRESS_NUMBER,1,255) as text(255) )                    as LONG_ADDRESS_NUMBER  ,
       cast(substring(a.SOURCE_NAME,1,255) as text(255) )                            as SOURCE_NAME,
       cast(substring(a.TAX_NUMBER,1,255) as text(255) )                             as TAX_NUMBER,
       cast(substring(a.ACTIVE_INDICATOR,1,255) as text(255) )                       as ACTIVE_INDICATOR
        
     from int A
            LEFT JOIN hist_customers B
            ON A.source_system_address_number = B.source_system_address_number and a.company_code = b.company_code
        /* This join is not done on GUID as the customer_address_number_guid is changed earlier to generate this we were not using company code but now using company code
    so we have to join on source_system_address_number and company_code */
),


old_dim as 
(
select 
        cast(substr(A.unique_key                         ,1,255)  as text(255))      as unique_key,
        cast(substr(A.customer_address_number_guid       ,1,255)  as text(255))      as customer_address_number_guid,
        cast(A.CUSTOMER_ADDRESS_NUMBER_GUID_OLD         AS NUMBER(38,0))           as CUSTOMER_ADDRESS_NUMBER_GUID_OLD,
        cast(substr(A.company_address_guid               ,1,255)  as text(255))      as company_address_guid,
        cast(A.COMPANY_ADDRESS_GUID_OLD  AS NUMBER(38,0))              as COMPANY_ADDRESS_GUID_OLD,
        cast(substr(A.GENERIC_ADDRESS_TYPE               ,1,255)  as text(255))      as GENERIC_ADDRESS_TYPE,
        cast(substr(A.source_system                      ,1,255)  as text(255))      as source_system,
        cast(substr(A.source_system_address_number       ,1,255)  as text(255))      as source_system_address_number,
        cast(substr(A.company_code                       ,1,255)  as text(255))      as company_code,
        cast(substr(A.company_name                       ,1,255)  as text(255))      as company_name,
        cast(substr(A.customer_name                      ,1,255)  as text(255))      as customer_name,
        cast(substr(A.customer_type                      ,1,255)  as text(255))      as customer_type,
        cast(substr(A.bill_to                            ,1,255)  as text(255))      as bill_to,
        cast(substr(A.bill_address_type                  ,1,255)  as text(255))      as bill_address_type,
        cast(substr(A.bill_address_type_description      ,1,255)  as text(255))      as bill_address_type_description,
        cast(substr(A.freight_handling_code              ,1,255)  as text(255))      as freight_handling_code,
        cast(substr(A.payment_terms_code                 ,1,255)  as text(255))      as payment_terms_code,
        cast(substr(A.payment_terms_description          ,1,255)  as text(255))      as payment_terms_description,
        cast(substr(A.csr_address_number                 ,1,255)  as text(255))      as csr_address_number,
        cast(substr(A.csr_name                           ,1,255)  as text(255))      as csr_name,
        cast(substr(A.credit_limit                       ,1,255)  as text(255))      as credit_limit,
        cast(substr(A.unified_customer                   ,1,255)  as text(255))      as unified_customer,
        cast(substr(A.shipping_terms                     ,1,255)  as text(255))      as shipping_terms,
        cast(substr(a.source_customer_code               ,1,255)  as text(255))      as source_customer_code,
        cast(substr(a.customer_group                     ,1,255)  as text(255))      as customer_group,
        cast(substr(a.customer_group_name                ,1,255)  as text(255))      as customer_group_name,
        cast(substr(a.legacy_customer_number             ,1,255)  as text(255))      as legacy_customer_number,
        cast(substr(a.currency_code                      ,1,255)  as text(255))      as currency_code,
        cast(substr(a.transport_mode                     ,1,255)  as text(255))      as transport_mode,
        cast(substr(a.bill_name                          ,1,255)  as text(255))      as bill_name,
        cast(substr(A.date_inserted                      ,1,255)  as text(255))      as date_inserted,
        cast(substr(A.date_updated                       ,1,255)  as text(255))      as date_updated,
        NULL                                                                         as customer_price_group,
        NULL                                                                         as channel,
        NULL                                                                         as invoice_method,
        'Y'                                                                          as CONV_STATUS,
    /*ADDRESS_FIELDS*/
        cast(NULL as text(255) )                                                     as address_line_1  ,
        cast(NULL as text(255) )                                                     as city  ,
        cast(NULL as text(255) )                                                     as state_province_code  ,
        cast(NULL as text(255) )                                                     as state_province  ,
        cast(NULL as text(255) )                                                     as country_code  ,
        cast(NULL as text(255) )                                                     as country  ,
	    cast(NULL as text(255) )                                                     as postal_code,
        cast(NULL as text(255) )                                                     as LONG_ADDRESS_NUMBER,
        cast(NULL as text(255) )                                                     as SOURCE_NAME  ,
        cast(NULL as text(255) )                                                     as TAX_NUMBER  ,
	    cast(NULL as text(255) )                                                     as ACTIVE_INDICATOR
     from hist_customers A
            Left JOIN int B
            ON A.source_system_address_number = B.source_system_address_number and a.company_code = b.company_code
             WHERE B.source_system_address_number is NULL
    /* This join is not done on GUID as the customer_address_number_guid is changed earlier to generate this we were not using company code but now using company code
    so we have to join on source_system_address_number and company_code */
),


Final_Dim 
as 
(
SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 Select * from Final_Dim 
