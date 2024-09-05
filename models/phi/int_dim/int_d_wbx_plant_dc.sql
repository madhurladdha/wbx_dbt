WITH STG_PLANT as (
    SELECT 'PLANT_DC' AS GENERIC_ADDRESS_TYPE,* FROM {{ ref('stg_d_wbx_plant_dc') }}
),

GUID_GENERATION as 
(
SELECT 
    GENERIC_ADDRESS_TYPE,
    {{ dbt_utils.surrogate_key(['SRC.SOURCE_SYSTEM','SRC.SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE']) }} AS PLANTDC_ADDRESS_GUID,
    SRC.SOURCE_SYSTEM_ADDRESS_NUMBER                         AS  v_ETL_EXCLUDE_FLAG,
    CASE WHEN v_ETL_EXCLUDE_FLAG='Y' THEN 'Y' ELSE 'N' END   AS   ETL_EXCLUDE_FLAG,          
    SRC.SOURCE_SYSTEM,
    SRC.BUSINESS_UNIT      AS SOURCE_BUSINESS_UNIT_CODE,
    SRC.BUSINESS_UNIT_NAME,
    SRC.REGION,
    SRC.ADDRESS_LINE_1,
    SRC.POSTAL_CODE,
    SRC.CITY,
    SRC.SOURCE_SYSTEM_ADDRESS_NUMBER,
    ACTIVE_CC_FLAG,
    CONSOLIDATED_SHIPMENT_DC_NAME,
    COUNTY,
    NVL(STATE_PROVINCE_CODE_LKP.NORMALIZED_VALUE,SRC.STATE_PROVINCE_CODE)     AS STATE_PROVINCE_CODE,               
    NVL(STATE_PROVINCE_LKP.NORMALIZED_VALUE,SRC.STATE_PROVINCE)               AS STATE_PROVINCE,                    
    NVL(COUNTRY_CODE_LKP.NORMALIZED_VALUE,SRC.COUNTRY_CODE)                   AS COUNTRY_CODE,                      
    NVL(COUNTRY_LKP.NORMALIZED_VALUE,SRC.COUNTRY)                             AS COUNTRY,        
    NVL(TO_CHAR(COMPANY_CODE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PLAN_COMPANY)) AS   COMPANY_CODE,
    NVL(TO_CHAR(COMPANY_NAME_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.PLAN_COMPANY)) AS   COMPANY_NAME,
    NVL(TO_CHAR(OPERATING_COMPANY_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)) AS OPERATING_COMPANY,
    NVL(TO_CHAR(SEGMENT_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER))   AS SEGMENT,
    NVL(TO_CHAR(TYPE_LKP.NORMALIZED_VALUE),TO_CHAR(SRC.TYPE))                              AS TYPE,
    TO_CHAR(systimestamp())               AS   DATE_INSERTED,
    TO_CHAR(systimestamp())               AS   DATE_UPDATED
        
    FROM stg_plant SRC
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COMPANY_CODE','UPPER(SRC.PLAN_COMPANY)','COMPANY_CODE_LKP') }}       
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COMPANY_CODE_NAME','UPPER(SRC.PLAN_COMPANY)','COMPANY_NAME_LKP') }}       
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','OPERATING_COMPANY','UPPER(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)','OPERATING_COMPANY_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK', 'SEGMENT','UPPER(SRC.SOURCE_SYSTEM_ADDRESS_NUMBER)','SEGMENT_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','TYPE','UPPER(SRC.TYPE)','TYPE_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','STATE_PROVINCE_CODE','UPPER(SRC.STATE_PROVINCE_CODE)','STATE_PROVINCE_CODE_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','STATE_PROVINCE','UPPER(SRC.STATE_PROVINCE)','STATE_PROVINCE_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COUNTRY_CODE','UPPER(SRC.COUNTRY_CODE)','COUNTRY_CODE_LKP') }}
    LEFT JOIN {{ lkp_normalization('SRC.SOURCE_SYSTEM','ADDRESS_BOOK','COUNTRY','UPPER(SRC.COUNTRY)','COUNTRY_LKP') }}
    ), 

INT_PLANT as 
(
    SELECT
        GENERIC_ADDRESS_TYPE,
        PLANTDC_ADDRESS_GUID,
        {{ dbt_utils.surrogate_key(['PLANTDC_ADDRESS_GUID']) }} as UNIQUE_KEY,
        ETL_EXCLUDE_FLAG, --Might not be needed
        GUID_GENERATION.SOURCE_SYSTEM,
        SOURCE_BUSINESS_UNIT_CODE,
        SOURCE_BUSINESS_UNIT_CODE AS BUSINESS_UNIT,
        BUSINESS_UNIT_NAME,
        TYPE,
        REGION,
        SOURCE_BUSINESS_UNIT_CODE AS BRANCH_OFFICE,
        BUSINESS_UNIT_NAME        AS BUSINESS_UNIT_LONG_DESCRIPTION, 
        TYPE                      AS DEPARTMENT_TYPE, 
        COMPANY_CODE,
        COMPANY_NAME,
        NVL(TO_CHAR(DIVISION_LKP.NORMALIZED_VALUE),TO_CHAR(COMPANY_CODE)) AS DIVISION,
        OPERATING_COMPANY,
        SEGMENT,	 
        SOURCE_SYSTEM_ADDRESS_NUMBER,
        ACTIVE_CC_FLAG,
        CONSOLIDATED_SHIPMENT_DC_NAME,
        COUNTY,
        ADDRESS_LINE_1,
        POSTAL_CODE,
        CITY,
        STATE_PROVINCE_CODE,
        STATE_PROVINCE,
        COUNTRY_CODE,
        COUNTRY,
        DATE_INSERTED,
        DATE_UPDATED
    from GUID_GENERATION
    LEFT JOIN {{ lkp_normalization('GUID_GENERATION.SOURCE_SYSTEM','ADDRESS_BOOK','DIVISION','UPPER(COMPANY_CODE)','DIVISION_LKP') }}       
),


HIST_PLANT as (
    select * from (select
        row_number()
            over (
                partition by SOURCE_BUSINESS_UNIT_CODE_NEW
                order by SOURCE_BUSINESS_UNIT_CODE desc
            )
            as ROWNUM,
        *
    from {{ ref('conv_dim_wbx_plant_dc') }})
    where ROWNUM = 1
),

FIN1 as (
    select
        GENERIC_ADDRESS_TYPE,
        UNIQUE_KEY,
        PLANTDC_ADDRESS_GUID,
        NULL as PLANTDC_ADDRESS_GUID_OLD,
        SOURCE_SYSTEM,
        SOURCE_BUSINESS_UNIT_CODE,
        BUSINESS_UNIT_NAME,
        TYPE,
        DIVISION,
        REGION,
        BRANCH_OFFICE,
        BUSINESS_UNIT_LONG_DESCRIPTION,
        DEPARTMENT_TYPE,
        COMPANY_CODE,
        COMPANY_NAME,
        CONSOLIDATED_SHIPMENT_DC_NAME,
        OPERATING_COMPANY,
        SEGMENT,
        SOURCE_SYSTEM_ADDRESS_NUMBER,
        ACTIVE_CC_FLAG,
        DATE_INSERTED,
        DATE_UPDATED,
        ETL_EXCLUDE_FLAG,
        COUNTY,
        ADDRESS_LINE_1,
        CITY,
        COUNTRY,
        COUNTRY_CODE,
        POSTAL_CODE,
        STATE_PROVINCE,
        STATE_PROVINCE_CODE,
        business_unit

    from INT_PLANT
),

NEW_DIM as (
    select distinct
        cast(substr(A.UNIQUE_KEY, 1, 255) as text(255)) as UNIQUE_KEY,
        cast(substr(A.GENERIC_ADDRESS_TYPE, 1, 255) as text(255))
            as GENERIC_ADDRESS_TYPE,
        cast(substr(A.PLANTDC_ADDRESS_GUID, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID,
        cast(substr(B.PLANTDC_ADDRESS_GUID, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID_OLD,
        cast(substr(A.SOURCE_SYSTEM, 1, 255) as text(255)) as SOURCE_SYSTEM,
        cast(substr(A.SOURCE_BUSINESS_UNIT_CODE, 1, 255) as text(255))
            as SOURCE_BUSINESS_UNIT_CODE,
        cast(substr(A.BUSINESS_UNIT_NAME, 1, 255) as text(255))
            as BUSINESS_UNIT_NAME,
        cast(substr(A.TYPE, 1, 255) as text(255)) as TYPE,
        cast(substr(A.DIVISION, 1, 255) as text(255)) as DIVISION,
        cast(substr(A.REGION, 1, 255) as text(255)) as REGION,
        cast(substr(A.BRANCH_OFFICE, 1, 255) as text(255)) as BRANCH_OFFICE,
        cast(substr(A.BUSINESS_UNIT_LONG_DESCRIPTION, 1, 255) as text(255))
            as BUSINESS_UNIT_LONG_DESCRIPTION,
        cast(substr(A.DEPARTMENT_TYPE, 1, 255) as text(255)) as DEPARTMENT_TYPE,
        cast(substr(A.CONSOLIDATED_SHIPMENT_DC_NAME, 1, 255) as text(255))
            as CONSOLIDATED_SHIPMENT_DC_NAME,
        cast(substr(A.COMPANY_CODE, 1, 255) as text(255)) as COMPANY_CODE,
        cast(substr(A.COMPANY_NAME, 1, 255) as text(255)) as COMPANY_NAME,
        cast(substr(A.OPERATING_COMPANY, 1, 255) as text(255))
            as OPERATING_COMPANY,
        cast(substr(A.SEGMENT, 1, 255) as text(255)) as SEGMENT,
        cast(substr(A.ACTIVE_CC_FLAG, 1, 255) as text(255)) as ACTIVE_CC_FLAG,
        cast(substr(A.ETL_EXCLUDE_FLAG, 1, 255) as text(255))
            as ETL_EXCLUDE_FLAG,
        cast(substr(A.DATE_INSERTED, 1, 255) as text(255)) as DATE_INSERTED,
        cast(substr(A.DATE_UPDATED, 1, 255) as text(255)) as DATE_UPDATED,
        /*ADDRESS FIELDS*/
        cast(substring(A.address_line_1,1,255) as text(255) ) as address_line_1,
        cast(substring(A.city,1,255) as text(255) ) as city  ,
        cast(substring(A.state_province_code,1,255) as text(255) ) as state_province_code,
        cast(substring(A.state_province,1,255) as text(255) ) as state_province,
        cast(substring(A.country_code,1,255) as text(255) ) as country_code,
        cast(substring(A.country,1,255) as text(255) ) as country,
	    cast(substring(A.postal_code,1,255) as text(255) ) as postal_code,
        cast(substring(A.business_unit,1,255) as text(255) ) as business_unit,
        cast(substring(A.COUNTY,1,255) as text(255) ) as COUNTY,
        cast(substring(A.SOURCE_SYSTEM_ADDRESS_NUMBER,1,255) as text(255) ) as SOURCE_SYSTEM_ADDRESS_NUMBER
        
    from FIN1 as A
    left join
        HIST_PLANT as B
        on A.PLANTDC_ADDRESS_GUID = B.PLANTDC_ADDRESS_GUID_NEW
),


OLD_DIM as (
    select distinct
        cast(substr(A.UNIQUE_KEY, 1, 255) as text(255)) as UNIQUE_KEY,
        cast(substr(A.GENERIC_ADDRESS_TYPE, 1, 255) as text(255))
            as GENERIC_ADDRESS_TYPE,
        cast(substr(A.PLANTDC_ADDRESS_GUID_NEW, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID,
        cast(substr(A.PLANTDC_ADDRESS_GUID, 1, 255) as text(255))
            as PLANTDC_ADDRESS_GUID_OLD,
        cast(substr(A.SOURCE_SYSTEM, 1, 255) as text(255)) as SOURCE_SYSTEM,
        cast(substr(A.SOURCE_BUSINESS_UNIT_CODE_NEW, 1, 255) as text(255))
            as SOURCE_BUSINESS_UNIT_CODE,
        cast(substr(A.BUSINESS_UNIT_NAME, 1, 255) as text(255))
            as BUSINESS_UNIT_NAME,
        cast(substr(A.TYPE, 1, 255) as text(255)) as TYPE,
        cast(substr(A.DIVISION, 1, 255) as text(255)) as DIVISION,
        cast(substr(A.REGION, 1, 255) as text(255)) as REGION,
        cast(substr(A.BRANCH_OFFICE, 1, 255) as text(255)) as BRANCH_OFFICE,
        cast(substr(A.BUSINESS_UNIT_LONG_DESCRIPTION, 1, 255) as text(255))
            as BUSINESS_UNIT_LONG_DESCRIPTION,
        cast(substr(A.DEPARTMENT_TYPE, 1, 255) as text(255)) as DEPARTMENT_TYPE,
        cast(substr(A.CONSOLIDATED_SHIPMENT_DC_NAME, 1, 255) as text(255))
            as CONSOLIDATED_SHIPMENT_DC_NAME,
        cast(substr(A.COMPANY_CODE, 1, 255) as text(255)) as COMPANY_CODE,
        cast(substr(A.COMPANY_NAME, 1, 255) as text(255)) as COMPANY_NAME,
        cast(substr(A.OPERATING_COMPANY, 1, 255) as text(255))
            as OPERATING_COMPANY,
        cast(substr(A.SEGMENT, 1, 255) as text(255)) as SEGMENT,
        cast(substr(A.ACTIVE_CC_FLAG, 1, 255) as text(255)) as ACTIVE_CC_FLAG,
        cast(substr(A.ETL_EXCLUDE_FLAG, 1, 255) as text(255))
            as ETL_EXCLUDE_FLAG,
        cast(substr(to_char(A.DATE_INSERTED), 1, 255) as text(255)) as DATE_INSERTED,
        cast(substr(to_char(A.DATE_UPDATED), 1, 255) as text(255)) as DATE_UPDATED,
        /*ADDRESS FIELDS*/
        cast(NULL as text(255) )  as address_line_1  ,
        cast(NULL as text(255) )  as city  ,
        cast(NULL as text(255) )  as state_province_code  ,
        cast(NULL as text(255) )  as state_province  ,
        cast(NULL as text(255) ) as country_code  ,
        cast(NULL as text(255) ) as country  ,
	    cast(NULL as text(255) )  as postal_code,
        cast(NULL as text(255) )  as BUSINESS_UNIT,
        cast(NULL as text(255) )  as COUNTY,
        cast(NULL as text(255) )  as SOURCE_SYSTEM_ADDRESS_NUMBER
    from HIST_PLANT as A
    left join
        FIN1 as B
        on A.PLANTDC_ADDRESS_GUID_NEW = B.PLANTDC_ADDRESS_GUID
    where B.PLANTDC_ADDRESS_GUID is NULL
),


FINAL as (
    select * from NEW_DIM
    union
    select * from OLD_DIM
)

select * from FINAL