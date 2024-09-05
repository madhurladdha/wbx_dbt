{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'UNIQUE_KEY',
    on_schema_change='sync_all_columns'
    )
}}


with hist_address as (
    select * from {{ ref('conv_dim_wbx_address') }} QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY source_business_unit_code) = 1
),


stg_address_master as (
    select  {{ dbt_utils.surrogate_key(['ADDRESS_GUID']) }}  AS UNIQUE_KEY,  * FROM {{ ref('stg_d_wbx_address') }}
),


final as (
SELECT
    NVL(A.UNIQUE_KEY,                     B.UNIQUE_KEY)                     AS UNIQUE_KEY,
    NVL(A.GENERIC_ADDRESS_TYPE,           B.GENERIC_ADDRESS_TYPE)           AS GENERIC_ADDRESS_TYPE,
    NVL(A.ADDRESS_GUID,             	  B.ADDRESS_GUID)                   AS ADDRESS_GUID,
    B.ADDRESS_GUID_OLD                                                      AS ADDRESS_GUID_OLD,
    NVL(A.SOURCE_SYSTEM,	              B.SOURCE_SYSTEM)                  AS SOURCE_SYSTEM,
    NVL(A.SOURCE_SYSTEM_ADDRESS_NUMBER,	  B.SOURCE_SYSTEM_ADDRESS_NUMBER)   AS SOURCE_SYSTEM_ADDRESS_NUMBER,
    NVL(A.ADDRESS_TYPE,	                  B.ADDRESS_TYPE)                   AS ADDRESS_TYPE,
    NVL(A.ADDRESS_TYPE_DESCRIPTION,	      B.ADDRESS_TYPE_DESCRIPTION)       AS ADDRESS_TYPE_DESCRIPTION,
    NVL(A.LONG_ADDRESS_NUMBER,	          B.LONG_ADDRESS_NUMBER)            AS LONG_ADDRESS_NUMBER,
    NVL(A.SOURCE_NAME,	                  B.SOURCE_NAME)                    AS SOURCE_NAME,
    NVL(A.DEPARTMENT_NAME,	              B.DEPARTMENT_NAME)                AS DEPARTMENT_NAME,
    NVL(A.SOURCE_BUSINESS_UNIT_CODE,	  B.SOURCE_BUSINESS_UNIT_CODE)      AS SOURCE_BUSINESS_UNIT_CODE,
    NVL(A.BUSINESS_UNIT,	              B.BUSINESS_UNIT)                  AS BUSINESS_UNIT,
    NVL(A.TAX_TYPE,	                      B.TAX_TYPE)                       AS TAX_TYPE,
    NVL(A.TAX_NUMBER,	                  B.TAX_NUMBER)                     AS TAX_NUMBER,
    NVL(A.CITY,	                          B.CITY)                           AS CITY,
    NVL(A.ADDRESS_LINE_1,	              B.ADDRESS_LINE_1)                 AS ADDRESS_LINE_1,
    NVL(A.ADDRESS_LINE_2,	              B.ADDRESS_LINE_2)                 AS ADDRESS_LINE_2,
    NVL(A.ADDRESS_LINE_3,	              B.ADDRESS_LINE_3)                 AS ADDRESS_LINE_3,
    NVL(A.ADDRESS_LINE_4,	              B.ADDRESS_LINE_4)                 AS ADDRESS_LINE_4,
    NVL(A.CONTACT_1_DEPARTMENT_NAME,	  B.CONTACT_1_DEPARTMENT_NAME)      AS CONTACT_1_DEPARTMENT_NAME,
    NVL(A.CONTACT_1_EMAIL,	              B.CONTACT_1_EMAIL)                AS CONTACT_1_EMAIL,
    NVL(A.CONTACT_1_FIRST_NAME,	          B.CONTACT_1_FIRST_NAME)           AS CONTACT_1_FIRST_NAME,
    NVL(A.CONTACT_1_LAST_NAME,	          B.CONTACT_1_LAST_NAME)            AS CONTACT_1_LAST_NAME,
    NVL(A.CONTACT_1_PRIMARY_PHONE_NUMBER, B.CONTACT_1_PRIMARY_PHONE_NUMBER) AS CONTACT_1_PRIMARY_PHONE_NUMBER,
    NVL(A.CONTACT_1_PRIMARY_PHONE_TYPE,	  B.CONTACT_1_PRIMARY_PHONE_TYPE)   AS CONTACT_1_PRIMARY_PHONE_TYPE,
    NVL(A.CONTACT_2_DEPARTMENT_NAME,	  B.CONTACT_2_DEPARTMENT_NAME)      AS CONTACT_2_DEPARTMENT_NAME,
    NVL(A.CONTACT_2_FIRST_NAME,	          B.CONTACT_2_FIRST_NAME)           AS CONTACT_2_FIRST_NAME,
    NVL(A.CONTACT_2_LAST_NAME,	          B.CONTACT_2_LAST_NAME)            AS CONTACT_2_LAST_NAME,
    NVL(A.CONTACT_2_PRIMARY_PHONE_NUMBER, B.CONTACT_2_PRIMARY_PHONE_NUMBER) AS CONTACT_2_PRIMARY_PHONE_NUMBER,
    NVL(A.CONTACT_2_PRIMARY_PHONE_TYPE,   B.CONTACT_2_PRIMARY_PHONE_TYPE)   AS CONTACT_2_PRIMARY_PHONE_TYPE,
    NVL(A.COUNTRY,	                      B.COUNTRY)                        AS COUNTRY,
    NVL(A.COUNTRY_CODE,	                  B.COUNTRY_CODE)                   AS COUNTRY_CODE,
    NVL(A.COUNTY,	                      B.COUNTY)                         AS COUNTY,
    systimestamp()                                                          AS DATE_INSERTED,
    systimestamp()                                                          AS DATE_UPDATED,
    NVL(A.POSTAL_CODE,	                  B.POSTAL_CODE)                    AS POSTAL_CODE,
    NVL(A.REQUIRED_1099,	              B.REQUIRED_1099)                  AS REQUIRED_1099,
    NVL(A.STATE_PROVINCE,	              B.STATE_PROVINCE)                 AS STATE_PROVINCE,
    NVL(A.STATE_PROVINCE_CODE,            B.STATE_PROVINCE_CODE)            AS STATE_PROVINCE_CODE,
    NVL(A.ACTIVE_INDICATOR,	              B.ACTIVE_INDICATOR)               AS ACTIVE_INDICATOR,
    NVL(B.SOURCE_PAYEE_ID,                A.SOURCE_PAYEE_ID)                AS SOURCE_PAYEE_ID,
    NVL(A.PAYEE_GUID,	                  B.PAYEE_GUID)                     AS PAYEE_GUID,
    B.PAYEE_GUID_OLD                                                        AS PAYEE_GUID_OLD,
    NVL(A.company_code ,                  B.company_code)                   AS COMPANY_CODE,
    CASE WHEN A.COMPANY_CODE IS NOT NULL THEN A.company_code_guid ELSE B.company_code_guid END As COMPANY_CODE_GUID
    /*replaced nvl with case staement as the company_code_guid is genertaed with md5 of companocde and source_system,
    it will never return null as source_system will be always prsent. */
FROM stg_address_master A
FULL OUTER JOIN hist_address B
ON A.ADDRESS_GUID = B.ADDRESS_GUID
)



select 
    cast(address_guid as text(255) ) as address_guid  ,
    cast(address_guid_old as text(255) ) as address_guid_old  ,
    cast(generic_address_type as text(255) ) as generic_Address_type, 
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(source_system_address_number,1,255) as text(255) ) as source_system_address_number  ,
    cast(substring(address_type,1,255) as text(255) ) as address_type  ,
    cast(substring(address_type_description,1,255) as text(255) ) as address_type_description  ,
    cast(substring(long_address_number,1,255) as text(255) ) as long_address_number  ,
    cast(substring(source_name,1,255) as text(255) ) as source_name  ,
    cast(substring(department_name,1,255) as text(255) ) as department_name  ,
    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,
    cast(substring(business_unit,1,255) as text(255) ) as business_unit  ,
    cast(substring(tax_type,1,255) as text(255) ) as tax_type  ,
    cast(substring(tax_number,1,255) as text(255) ) as tax_number  ,
    cast(substring(address_line_1,1,255) as text(255) ) as address_line_1  ,
    cast(substring(address_line_2,1,255) as text(255) ) as address_line_2  ,
    cast(substring(address_line_3,1,255) as text(255) ) as address_line_3  ,
    cast(substring(address_line_4,1,255) as text(255) ) as address_line_4  ,
    cast(substring(postal_code,1,255) as text(255) ) as postal_code  ,
    cast(substring(city,1,255) as text(255) ) as city  ,
    cast(substring(county,1,255) as text(255) ) as county  ,
    cast(substring(state_province_code,1,255) as text(255) ) as state_province_code  ,
    cast(substring(state_province,1,255) as text(255) ) as state_province  ,
    cast(substring(country_code,1,255) as text(255) ) as country_code  ,
    cast(substring(country,1,255) as text(255) ) as country  ,
    cast(substring(contact_1_first_name,1,255) as text(255) ) as contact_1_first_name  ,
    cast(substring(contact_1_last_name,1,255) as text(255) ) as contact_1_last_name  ,
    cast(substring(contact_1_primary_phone_type,1,255) as text(255) ) as contact_1_primary_phone_type  ,
    cast(substring(contact_1_primary_phone_number,1,255) as text(255) ) as contact_1_primary_phone_number  ,
    cast(substring(contact_1_department_name,1,255) as text(255) ) as contact_1_department_name  ,
    cast(substring(contact_1_email,1,255) as text(255) ) as contact_1_email  ,
    cast(substring(contact_2_first_name,1,255) as text(255) ) as contact_2_first_name  ,
    cast(substring(contact_2_last_name,1,255) as text(255) ) as contact_2_last_name  ,
    cast(substring(contact_2_primary_phone_type,1,255) as text(255) ) as contact_2_primary_phone_type  ,
    cast(substring(contact_2_primary_phone_number,1,255) as text(255) ) as contact_2_primary_phone_number  ,
    cast(substring(contact_2_department_name,1,255) as text(255) ) as contact_2_department_name  ,
    cast(substring(systimestamp(),1,255) as text(255) ) as date_inserted  ,
    cast(substring(systimestamp(),1,255) as text(255) ) as date_updated  ,
    cast(substring(required_1099,1,255) as text(255) ) as required_1099  ,
    cast(substring(active_indicator,1,255) as text(255) ) as active_indicator  ,
    cast(substring(source_payee_id,1,255) as text(255) ) as source_payee_id  ,
    cast(payee_guid as text(255) ) as payee_guid  ,
    cast(payee_guid_old as text(255) ) as payee_guid_old  ,
    cast(unique_key as text(255) ) as unique_key,
    cast(company_code as text(255)) as company_code,
    cast(company_code_guid as text(255)) as company_code_guid
     from final
     qualify row_number() over(partition by unique_key order by 1)=1