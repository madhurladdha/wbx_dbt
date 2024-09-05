
with stg_account as (
    select * from {{ ref('stg_d_wbx_account') }}
),

stg_onestream as (
    select * from {{ ref('stg_d_wbx_fin_onestream') }}

),

old_account_dim as
   (
      select * from (select
        row_number()
            over (
                partition by unique_key
                order by SOURCE_OBJECT_ID desc
            )
            as ROWNUM,
        *
    from {{ ref('conv_dim_wbx_account') }})
    where ROWNUM = 1
    ),


account as 
(
	select distinct SOURCE_SYSTEM,source_object_id,source_company_code,source_subsidiary_id,source_account_identifier,source_concat_nat_key,source_business_unit_code
    from  stg_account
union
	   select distinct SOURCE_SYSTEM,source_object_id,source_company_code,source_subsidiary_id,source_account_identifier,source_concat_nat_key,source_business_unit_code
       from  old_account_dim 
),

--------using conv model to get existing records from account(cann't use final dim account model due to circular logic issue)
consolidation_stg as(
select 
account.source_company_code,
account.SOURCE_BUSINESS_UNIT_CODE,
FOS.SOURCE_SYSTEM,
FOS.WORKFLOW_PROFILE,
FOS.SOURCE_IC,
FOS.TARGET_IC,
FOS.SOURCE_ACCOUNT,
FOS.TARGET_ACCOUNT,
FOS.SCENARIO,
FOS.TIME,
FOS.SOURCE_MAINUD1,
FOS.TARGET_MAINUD1,
FOS.SOURCE_MAINUD2,
FOS.TARGET_MAINUD2,
FOS.SOURCE_MAINUD3,
FOS.TARGET_MAINUD3,
FOS.SOURCEID,
FOS.SOURCE_DESC,
FOS.VIEW,
FOS.SOURCE_ENTITY,
FOS.TARGET_ENTITY,
FOS.SOURCE_FLOW,
FOS.TARGET_FLOW,
FOS.ORIGIN,
FOS.AMOUNT,
FOS.LOAD_DATE,
FOS.FILE_NAME
from stg_onestream FOS
inner join account
ON     account.SOURCE_OBJECT_ID = FOS.SOURCE_ACCOUNT
AND    account.SOURCE_BUSINESS_UNIT_CODE = (CASE WHEN FOS.SOURCE_MAINUD1 IN('None', 'No_CC', '%')THEN '-' ELSE FOS.SOURCE_MAINUD1 END)

),


stg as 
(
SELECT 
source_system,
max(workflow_profile) as workflow_profile,
source_ic as source_entity,
source_account,
CAST (TRIM (source_company_code)|| '.'|| COALESCE (NULLIF(TRIM(source_business_unit_code),''), '-') || '.'|| TRIM (SOURCE_account) AS VARCHAR2 (60)) AS SOURCE_CONCAT_NAT_KEY,
max(SOURCE_MAINUD1) as SOURCE_MAINUD1,
MAX (target_Account) AS target_Account,
MAX (TARGET_MAINUD1) AS TARGET_MAINUD1,
MAX (SOURCEID)     AS SOURCEID
FROM consolidation_stg
WHERE WORKFLOW_PROFILE LIKE 'WeetabixUK%' and UPPER(TRIM(SCENARIO)) <> 'BUDGET'
GROUP BY source_system,source_ic,source_account,source_company_code,source_business_unit_code,CAST (TRIM (source_company_code)|| '.'|| COALESCE (NULLIF(TRIM(SOURCE_MAINUD1),''), '-') || '.'|| TRIM (SOURCE_account) AS VARCHAR2 (60))
),


final as
    (
        SELECT 
            source_system,
            {{ dbt_utils.surrogate_key(['source_system','source_concat_nat_key']) }} as uid,
            source_concat_nat_key,
            source_account,
            source_entity,
            source_mainud1,
            TARGET_ACCOUNT,
            TARGET_MAINUD1,
            SOURCEID,
            workflow_profile
        FROM stg
    )
    
select  * from final