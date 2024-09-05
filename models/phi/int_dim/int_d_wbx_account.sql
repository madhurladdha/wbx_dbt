with SRC as (
    select * from {{ ref( 'stg_d_wbx_account')}}
),


NORMALIZATION_WAVE1 as (
    select

        SRC.SOURCE_SYSTEM,
        {{ dbt_utils.surrogate_key(['src.source_system','src.source_concat_nat_key']) }}
            as ACCOUNT_GUID,
        SOURCE_CONCAT_NAT_KEY,
        SOURCE_SUBSIDIARY_ID,
        SOURCE_OBJECT_ID,
        SOURCE_COMPANY_CODE,
        SOURCE_BUSINESS_UNIT_CODE,
        SOURCE_ACCOUNT_IDENTIFIER,
        NVL(
            TO_CHAR(ACCOUNT_TYPE_LKP.NORMALIZED_VALUE),
            TO_CHAR(SRC.ACCOUNT_TYPE)
        ) as ACCOUNT_TYPE,
        ACCOUNT_LEVEL,
        NVL(
            TO_CHAR(ACCOUNT_CATEGORY_LKP.NORMALIZED_VALUE),
            TO_CHAR(SRC.ACCOUNT_CATEGORY)
        ) as ACCOUNT_CATEGORY,
        ACCOUNT_SUBCATEGORY_LKP.NORMALIZED_VALUE as ACCOUNT_SUBCATEGORY,
        ACCOUNT_DESCRIPTION,
        '8675309' as TAGETIK_ACCOUNT,
        ENTRY_ALLOWED_FLAG

    from SRC
    left join
        {{ lkp_normalization('SRC.SOURCE_SYSTEM','ACCOUNT','ACCOUNT_TYPE','UPPER(SRC.ACCOUNT_TYPE)','ACCOUNT_TYPE_LKP') }}
    left join
        {{ lkp_normalization('SRC.SOURCE_SYSTEM','ACCOUNT','ACCOUNT_CATEGORY','UPPER(SRC.ACCOUNT_Category)','ACCOUNT_CATEGORY_LKP') }}
    left join
        {{ lkp_normalization('SRC.SOURCE_SYSTEM','ACCOUNT','ACCOUNT_SUBCATEGORY','UPPER(SRC.ACCOUNT_SUBCATEGORY)','ACCOUNT_SUBCATEGORY_LKP') }}
)

select * from NORMALIZATION_WAVE1