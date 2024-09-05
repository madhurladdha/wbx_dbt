WITH VENDTABLE as (
    SELECT * FROM {{ ref('src_vendtable') }}
),

VENDGROUP as (
    SELECT * FROM {{ ref('src_vendgroup') }}
),
DIRPARTYTABLE as (
    SELECT * FROM {{ ref('src_dirpartytable') }}
),

LOGPOSADD as (
    SELECT * FROM {{ ref('src_logisticspostaladdress') }} WHERE CURRENT_TIMESTAMP BETWEEN VALIDFROM AND VALIDTO
),
LOGADDSTATE as (
    SELECT * FROM {{ ref('src_logisticsaddressstate') }}
),

FINAL as
(
    SELECT DISTINCT '{{env_var("DBT_SOURCE_SYSTEM")}}'  AS SOURCE_SYSTEM,
                    upper(VT.DATAAREAID)             AS Company_code,
                    VT.ACCOUNTNUM             AS SOURCE_SYSTEM_ADDRESS_NUMBER,
                    DPT.NAME                  AS SOURCE_NAME,
                    'SUPP'                    AS ADDRESS_TYPE,
                    'SUPPLIER'                AS ADDRESS_TYPE_DESCRIPTION,
                    LPA.STREET                AS ADDRESS_LINE_1,
                    LPA.CITY                  AS CITY,
                    LPA.STATE                 AS STATE_PROVINCE_CODE,
                    NVL (LAS.NAME, LPA.STATE) AS STATE_PROVINCE,
                    LPA.ZIPCODE               AS POSTAL_CODE,
                    LPA.COUNTRYREGIONID       AS COUNTRY_CODE,
                    LPA.COUNTRYREGIONID       AS COUNTRY,
                    DPT.PARTYNUMBER           AS LONG_ADDRESS_NUMBER,
                    DPT.NAME                  AS SUPPLIER_NAME,
                    NULL                      AS SUPPLIER_TYPE,
                    VT.PAYMTERMID             AS SUPP_PAYMENT_TERMS_CODE,
                    VT.PAYMTERMID             AS SUPP_PAYMENT_TERMS_DESC,
                    VT.PAYMMODE               AS PAYMENT_INSTRUMENT_CODE,
                    VT.PAYMMODE               AS PAYMENT_INSTRUMENT_NAME,
                    VT.CURRENCY               AS SUPP_CURRENCY_CODE,
                    VT.DLVMODE                AS SUPP_TRANSPORT_MODE,
                    VT.DLVMODE                AS SUPP_TRANSPORT_MODE_DESC,
                    NULL                      AS ACCT_PAY_GL_ACCT,
                    NULL                      AS CUSTOMER_FLAG,
                    VT.PURCHPOOLID            AS SOURCE_SUPPLIER_TYPE, ---Change on 9/27/18 by Mike Traub TRIM (NVL (VT.SUBSEGMENTID, '-')) AS SOURCE_SUPPLIER_TYPE,
                    VT.VATNUM                 AS TAX_NUMBER,
                    VT.DLVTERM                AS SUPP_SHIPPING_TERMS,
                    CAST (SYSTIMESTAMP() AS VARCHAR2 (255))      AS DATE_INSERTED,
                    CAST (SYSTIMESTAMP() AS VARCHAR2 (255))      AS DATE_UPDATED,
                    NULL                      AS VOUCHER_DATE
      FROM VENDTABLE  VT
      INNER JOIN DIRPARTYTABLE DPT ON VT.PARTY = DPT.RECID
      INNER JOIN VENDGROUP VG ON     VT.DATAAREAID = VG.DATAAREAID AND VT.VENDGROUP = VG.VENDGROUP
      LEFT OUTER JOIN LOGPOSADD  LPA ON DPT.PRIMARYADDRESSLOCATION = LPA.LOCATION
      LEFT OUTER JOIN LOGADDSTATE LAS ON     LPA.COUNTRYREGIONID = LAS.COUNTRYREGIONID AND LPA.STATE = LAS.STATEID
     -- WHERE (VT.ACCOUNTNUM <> '9900000' OR UPPER (TRIM (VT.DATAAREAID)) <> 'RFL')
     /* Above condition is removed as part of D365 work were 1 supplier id can be part of 2 company code. 
     This condition is removed thinking this was added earlier in the model as the company was not part of key */ 
)

select * from FINAL --qualify ROW_NUMBER() over(partition by SOURCE_SYSTEM_ADDRESS_NUMBER order by company_code desc)=1