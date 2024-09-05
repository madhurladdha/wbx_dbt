with CUSTTABLE as (
    select * from {{ ref('src_custtable') }}
),

DIRPARTYTABLE as (
    select * from {{ ref('src_dirpartytable') }}
),

DATAAREA as (
    select * from {{ ref('src_dataarea') }}
),

LOGISTICSPOSTALADDRESS as (
    select * from {{ ref('src_logisticspostaladdress') }} where CURRENT_TIMESTAMP between VALIDFROM and VALIDTO
),

LOGISTICSADDRESSSTATE as (
    select * from {{ ref('src_logisticsaddressstate') }}
),

HCMWORKER as (
    select * from {{ ref('src_hcmworker') }}
),

BCT as (
    select
        A.ACCOUNTNUM,
        B.NAME,
        A.DATAAREAID
    from CUSTTABLE as A, DIRPARTYTABLE as B where A.PARTY = B.RECID
    qualify ROW_NUMBER() over (partition by A.ACCOUNTNUM, A.DATAAREAID order by A.SOURCE asc) = 1
),

CSR as (
    select
        A.RECID,
        B.NAME,
        A.PERSONNELNUMBER
    from HCMWORKER as A, DIRPARTYTABLE as B where A.PERSON = B.RECID
),



FINAL as (
    select
        '{{ env_var("DBT_SOURCE_SYSTEM") }}' as SOURCE_SYSTEM,
        CT.ACCOUNTNUM as SOURCE_SYSTEM_ADDRESS_NUMBER,
        DPT.NAME as SOURCE_NAME,
        'CUST' as ADDRESS_TYPE,
        'CUST' as ADDRESS_TYPE_DESCRIPTION,
        LPA.STREET as ADDRESS_LINE_1,
        LPA.CITY as CITY,
        LPA.STATE as STATE_PROVINCE_CODE,
        NVL(LAS.NAME, LPA.STATE) as STATE_PROVINCE,
        LPA.ZIPCODE as POSTAL_CODE,
        LPA.COUNTRYREGIONID as COUNTRY_CODE,
        LPA.COUNTRYREGIONID as COUNTRY,
        'Y' as ACTIVE_INDICATOR,
        CT.VATNUM as TAX_NUMBER,
        DPT.PARTYNUMBER as LONG_ADDRESS_NUMBER,
        TRIM(UPPER(CT.DATAAREAID)) as CUST_COMPANY,
        DA.NAME as CUST_COMPANY_NAME,
        DPT.NAME as CUSTOMER_NAME,
        case when ((TRIM(CT.INVOICEACCOUNT) = '') or TRIM(CT.INVOICEACCOUNT) is NULL) then 'CB' else 'CS' end as CUSTOMER_TYPE,
        CT.INVOICEACCOUNT as BILL_TO,
        BCT.NAME as BILL_NAME,
        CT.CUSTGROUP as CUSTOMER_GROUP,
        CT.CUSTGROUP as CUSTOMER_GROUP_NAME,
        CT.ACCOUNTNUM as LEGACY_CUSTOMER_NUMBER,
        CT.CURRENCY as CUST_CURRENCY_CODE,
        CT.DLVMODE as CUST_TRANSPORT_MODE,
        CT.DLVTERM as CUST_SHIPPING_TERMS,
        CT.PAYMTERMID as CUST_PAYMENT_TERMS_CODE,
        CT.PAYMTERMID as CUST_PAYMENT_TERMS_DESC,
        CSR.PERSONNELNUMBER as CSR_ADDRESS_NUMBER,
        CSR.NAME as CSR_NAME,
        CT.CREDITMAX as CREDIT_LIMIT,
        TRIM(UPPER(CT.DATAAREAID)) as PLAN_COMPANY,
        DA.NAME as PLAN_COMPANY_NAME,
        CT.ACCOUNTNUM as SOURCE_CUSTOMER_CODE
    from CUSTTABLE as CT
    inner join DIRPARTYTABLE as DPT on CT.PARTY = DPT.RECID
    inner join DATAAREA as DA on UPPER(TRIM(CT.DATAAREAID)) = UPPER(TRIM(DA.ID))
    left outer join LOGISTICSPOSTALADDRESS as LPA on DPT.PRIMARYADDRESSLOCATION = LPA.LOCATION
    left outer join LOGISTICSADDRESSSTATE as LAS on LPA.COUNTRYREGIONID = LAS.COUNTRYREGIONID and LPA.STATE = LAS.STATEID
    left outer join BCT on CT.INVOICEACCOUNT = BCT.ACCOUNTNUM and CT.DATAAREAID = BCT.DATAAREAID
    left outer join CSR on CT.MAINCONTACTWORKER = CSR.RECID
    where not (TRIM(UPPER(CT.ACCOUNTNUM)) = '6630642-0000' and TRIM(UPPER(CT.DATAAREAID)) = 'RFL')

)

select * from FINAL
