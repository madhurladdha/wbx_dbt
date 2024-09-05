{{
    config(
        materialized = env_var('DBT_MAT_INCREMENTAL'),
        transient = false,
        unique_key = 'GENERATED_ADDRESS_NUMBER',
        on_schema_change='sync_all_columns'
    )
}}

with vendors as (
    select {{ dbt_utils.surrogate_key(['SOURCE_SYSTEM','SOURCE_SYSTEM_ADDRESS_NUMBER','GENERIC_ADDRESS_TYPE','COMPANY_CODE']) }}  AS GENERATED_ADDRESS_NUMBER,*
     from {{ ref('int_d_wbx_supplier') }}
),
old_vendors as (
    select * from {{ ref('conv_dim_wbx_prc_supplier_categorization') }}
),
normalized_vendors as (
    select
        {{ dbt_utils.surrogate_key(['SRC.GENERATED_ADDRESS_NUMBER']) }}  AS UNIQUE_KEY,
        GENERATED_ADDRESS_NUMBER,
        SOURCE_SYSTEM_ADDRESS_NUMBER,
        SRC.SOURCE_SYSTEM,
        SRC.ADDRESS_LINE_1,
        SOURCE_NAME,
        NULL    AS GENERATED_ADDRESS_NUMBER_OLD,
        PHI_SUPPLIER_TYPE AS SUPPLIER_TYPE,
        CAST(systimestamp()   AS  TIMESTAMP_NTZ(9))          AS DATE_INSERTED,
        CAST(systimestamp()    AS TIMESTAMP_NTZ(9))         AS DATE_UPDATED,
        COMPANY_CODE
        from vendors SRC
   
),

new_dim as (

		SELECT
		A.UNIQUE_KEY,
		A.GENERATED_ADDRESS_NUMBER,
		B.GENERATED_ADDRESS_NUMBER_OLD,
		A.SOURCE_SYSTEM,
		A.SOURCE_SYSTEM_ADDRESS_NUMBER,
		A.SOURCE_NAME,
		A.ADDRESS_LINE_1,
		A.SUPPLIER_TYPE,
		A.DATE_INSERTED,
		A.DATE_UPDATED,
        A.COMPANY_CODE
		FROM normalized_vendors A left join old_vendors B on A.UNIQUE_KEY=B.UNIQUE_KEY 
),

old_dim as (

		SELECT
		A.UNIQUE_KEY,
		A.GENERATED_ADDRESS_NUMBER,
		A.GENERATED_ADDRESS_NUMBER_OLD,
		A.SOURCE_SYSTEM,
		A.SOURCE_SYSTEM_ADDRESS_NUMBER,
		A.SOURCE_NAME,
		A.ADDRESS_LINE_1,
		A.SUPPLIER_TYPE,
		A.DATE_INSERTED,
		A.DATE_UPDATED,
        A.COMPANY_CODE
		FROM old_vendors A left join normalized_vendors B on A.UNIQUE_KEY=B.UNIQUE_KEY 
		WHERE B.UNIQUE_KEY  IS NULL

),

final_dim as (
select * from new_dim
union
select * from old_dim

)

select * from final_dim