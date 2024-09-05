WITH onestream as
( SELECT * FROM {{ ref('dim_wbx_fin_onestream') }}),

Hierarchy_Non_Balance_Sheet as 
(SELECT TAGETIK_ACCOUNT FROM {{ ref('xref_wbx_hierarchy') }} WHERE SOURCE_SYSTEM IN ('TAGETIK_ACCOUNT','EBITDA_HIER') AND NODE_1 <> 'BalanceSheet' GROUP BY TAGETIK_ACCOUNT),

Hierarchy_Balance_Sheet as 
(SELECT TAGETIK_ACCOUNT FROM {{ ref('xref_wbx_hierarchy') }} WHERE SOURCE_SYSTEM IN ('TAGETIK_ACCOUNT','EBITDA_HIER') AND NODE_1 = 'BalanceSheet' GROUP BY TAGETIK_ACCOUNT),

Current_Month_Non_Balance_Sheet as
(SELECT * FROM        
    (SELECT *,
    RANK() OVER (PARTITION BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0))
                 ORDER BY VERSION_DATE DESC) AS VERSION_RANK,
    DENSE_RANK() OVER (ORDER BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0)) DESC) AS TIME_RANK
    FROM onestream
    INNER JOIN Hierarchy_Non_Balance_Sheet HR
        ON onestream.TARGET_ACCOUNT = HR.TAGETIK_ACCOUNT
    WHERE TIME IS NOT NULL) ---filter out balance sheet accounts
    WHERE VERSION_RANK = 1
    AND TIME_RANK = 1),

Prior_Month_Non_Balance_Sheet as 
(SELECT * FROM        
    (SELECT *,
    RANK() OVER (PARTITION BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0))
                 ORDER BY VERSION_DATE DESC) AS VERSION_RANK,
    DENSE_RANK() OVER (ORDER BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0)) DESC) AS TIME_RANK
    FROM onestream
    INNER JOIN  Hierarchy_Non_Balance_Sheet HR
        ON onestream.TARGET_ACCOUNT = HR.TAGETIK_ACCOUNT
    WHERE TIME IS NOT NULL)  ---filter out balance sheet accounts
    WHERE VERSION_RANK = 1
    AND TIME_RANK = 2),

Current_Month_Balance_Sheet as
(SELECT * FROM        
    (SELECT *,
    RANK() OVER (PARTITION BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0))
                 ORDER BY VERSION_DATE DESC) AS VERSION_RANK,
    DENSE_RANK() OVER (ORDER BY TO_NUMBER(LEFT(TIME,4)||LPAD(SUBSTR(TIME,POSITION('M',TIME,1)+1,LENGTH(TIME)-1),2,0)) DESC) AS TIME_RANK
    FROM onestream
    INNER JOIN Hierarchy_Balance_Sheet HR
        ON onestream.TARGET_ACCOUNT = HR.TAGETIK_ACCOUNT
    WHERE TIME IS NOT NULL)
    WHERE VERSION_RANK = 1
    AND TIME_RANK = 1),    

-----logic to generate monthly amount by substracting current month YTD from last month YTD for non balance sheet accounts ------
Monthly_Amt_Non_Balance_Sheet AS
(
    SELECT
COALESCE(CURRENT_MONTH.WORKFLOW_PROFILE,PRIOR_MONTH.WORKFLOW_PROFILE) AS WORKFLOW_PROFILE,
COALESCE(CURRENT_MONTH.SOURCEID,PRIOR_MONTH.SOURCEID) AS SOURCEID,
COALESCE(CURRENT_MONTH.SOURCE_DESC, PRIOR_MONTH.SOURCE_DESC) AS SOURCE_DESC,
COALESCE (LEFT(CURRENT_MONTH.TIME,4)||LPAD(SUBSTR(CURRENT_MONTH.TIME,POSITION('M',CURRENT_MONTH.TIME,1)+1,LENGTH(CURRENT_MONTH.TIME)-1),2,0),
         TO_CHAR(TO_NUMBER(LEFT(PRIOR_MONTH.TIME,4)||LPAD(SUBSTR(PRIOR_MONTH.TIME,POSITION('M',PRIOR_MONTH.TIME,1)+1,LENGTH(PRIOR_MONTH.TIME)-1),2,0)+1))
         ) AS FISCAL_PERIOD,
COALESCE(CURRENT_MONTH.SOURCE_ENTITY, PRIOR_MONTH.SOURCE_ENTITY) AS SOURCE_ENTITY,
COALESCE(CURRENT_MONTH.TARGET_ENTITY, PRIOR_MONTH.TARGET_ENTITY) AS TARGET_ENTITY,
COALESCE(CURRENT_MONTH.SOURCE_ACCOUNT, PRIOR_MONTH.SOURCE_ACCOUNT) AS SOURCE_ACCOUNT,
COALESCE(CURRENT_MONTH.TARGET_ACCOUNT, PRIOR_MONTH.TARGET_ACCOUNT) AS TARGET_ACCOUNT,
COALESCE(CURRENT_MONTH.SOURCE_IC, PRIOR_MONTH.SOURCE_IC) AS SOURCE_IC,
COALESCE(CURRENT_MONTH.TARGET_IC, PRIOR_MONTH.TARGET_IC) AS TARGET_IC,
COALESCE(CURRENT_MONTH.SOURCE_MAINUD1, PRIOR_MONTH.SOURCE_MAINUD1) AS SOURCE_MAINUD1,
COALESCE(CURRENT_MONTH.TARGET_MAINUD1, PRIOR_MONTH.TARGET_MAINUD1) AS TARGET_MAINUD1,
COALESCE(CURRENT_MONTH.SOURCE_MAINUD2, PRIOR_MONTH.SOURCE_MAINUD2) AS SOURCE_MAINUD2,
COALESCE(CURRENT_MONTH.TARGET_MAINUD2, PRIOR_MONTH.TARGET_MAINUD2) AS TARGET_MAINUD2,
COALESCE(CURRENT_MONTH.SOURCE_MAINUD3, PRIOR_MONTH.SOURCE_MAINUD3) AS SOURCE_MAINUD3,
COALESCE(CURRENT_MONTH.TARGET_MAINUD3, PRIOR_MONTH.TARGET_MAINUD3) AS TARGET_MAINUD3,
ROUND(NVL(CURRENT_MONTH.AMOUNT,0) - NVL(PRIOR_MONTH.AMOUNT,0),8) AS MONTHLY_AMOUNT
FROM   Current_Month_Non_Balance_Sheet current_month
FULL JOIN Prior_Month_Non_Balance_Sheet prior_month
  ------Full outer is needed because if an account is removed it will no longer be provided in file-------
ON NVL(CURRENT_MONTH.WORKFLOW_PROFILE,'-') = NVL(PRIOR_MONTH.WORKFLOW_PROFILE,'-')
AND NVL(CURRENT_MONTH.SOURCE_ENTITY,'-') = NVL(PRIOR_MONTH.SOURCE_ENTITY,'-')
AND NVL(CURRENT_MONTH.SOURCE_ACCOUNT,'-') = NVL(PRIOR_MONTH.SOURCE_ACCOUNT,'-')
AND NVL(CURRENT_MONTH.TARGET_ACCOUNT,'-') = NVL(PRIOR_MONTH.TARGET_ACCOUNT,'-')
AND NVL(CURRENT_MONTH.SOURCE_IC,'-') = NVL(PRIOR_MONTH.SOURCE_IC,'-')
AND NVL(CURRENT_MONTH.TARGET_IC,'-') = NVL(PRIOR_MONTH.TARGET_IC,'-')
AND NVL(CURRENT_MONTH.SOURCE_MAINUD1,'-') = NVL(PRIOR_MONTH.SOURCE_MAINUD1,'-')
AND NVL(CURRENT_MONTH.SOURCE_MAINUD2,'-') = NVL(PRIOR_MONTH.SOURCE_MAINUD2,'-')
AND NVL(CURRENT_MONTH.SOURCE_MAINUD3,'-') = NVL(PRIOR_MONTH.SOURCE_MAINUD3,'-')
AND NVL(CURRENT_MONTH.TARGET_MAINUD1,'-') = NVL(PRIOR_MONTH.TARGET_MAINUD1,'-')
AND NVL(CURRENT_MONTH.TARGET_MAINUD2,'-') = NVL(PRIOR_MONTH.TARGET_MAINUD2,'-')
AND NVL(CURRENT_MONTH.TARGET_MAINUD3,'-') = NVL(PRIOR_MONTH.TARGET_MAINUD3,'-')
AND NVL(CURRENT_MONTH.SOURCE_DESC,'-') = NVL(PRIOR_MONTH.SOURCE_DESC,'-')
AND LEFT(CURRENT_MONTH.TIME,4) = LEFT(PRIOR_MONTH.TIME,4)
  WHERE COALESCE(LEFT(CURRENT_MONTH.TIME,4), LEFT(PRIOR_MONTH.TIME,4)) =
  (SELECT LEFT(TIME,4) FROM        
          (SELECT * FROM Current_Month_Non_Balance_Sheet) GROUP BY TIME)
),

--------add in Balance sheet accounts from the current month--------
Monthly_Amt_Balance_Sheet AS
(SELECT
CURRENT_MONTH.WORKFLOW_PROFILE,
CURRENT_MONTH.SOURCEID,
CURRENT_MONTH.SOURCE_DESC,
LEFT(CURRENT_MONTH.TIME,4)||LPAD(SUBSTR(CURRENT_MONTH.TIME,POSITION('M',CURRENT_MONTH.TIME,1)+1,LENGTH(CURRENT_MONTH.TIME)-1),2,0) AS FISCAL_PERIOD,
CURRENT_MONTH.SOURCE_ENTITY,
CURRENT_MONTH.TARGET_ENTITY,
CURRENT_MONTH.SOURCE_ACCOUNT,
CURRENT_MONTH.TARGET_ACCOUNT,
CURRENT_MONTH.SOURCE_IC,
CURRENT_MONTH.TARGET_IC,
CURRENT_MONTH.SOURCE_MAINUD1,
CURRENT_MONTH.TARGET_MAINUD1,
CURRENT_MONTH.SOURCE_MAINUD2,
CURRENT_MONTH.TARGET_MAINUD2,
CURRENT_MONTH.SOURCE_MAINUD3,
CURRENT_MONTH.TARGET_MAINUD3,
ROUND(CURRENT_MONTH.AMOUNT,8) AS MONTHLY_AMOUNT
FROM Current_Month_Balance_Sheet CURRENT_MONTH
),

Final as
(select 
{{ dbt_utils.surrogate_key(['WORKFLOW_PROFILE','SOURCEID','SOURCE_ENTITY','TARGET_ENTITY','SOURCE_ACCOUNT','TARGET_ACCOUNT','SOURCE_IC','TARGET_IC','SOURCE_MAINUD1','TARGET_MAINUD1','SOURCE_MAINUD2','TARGET_MAINUD2','SOURCE_MAINUD3','TARGET_MAINUD3','FISCAL_PERIOD']) }} AS UNIQUE_KEY,
WORKFLOW_PROFILE,
SOURCEID,
SOURCE_DESC,
FISCAL_PERIOD,
SOURCE_ENTITY,
TARGET_ENTITY,
SOURCE_ACCOUNT,
TARGET_ACCOUNT,
SOURCE_IC,
TARGET_IC,
SOURCE_MAINUD1,
TARGET_MAINUD1,
SOURCE_MAINUD2,
TARGET_MAINUD2,
SOURCE_MAINUD3,
TARGET_MAINUD3,
MONTHLY_AMOUNT,
cast(CURRENT_DATE as timestamp_ntz) AS LOAD_DATE,
cast(CURRENT_DATE as timestamp_ntz) AS UPDATE_DATE
from Monthly_Amt_Non_Balance_Sheet
UNION all

select
{{ dbt_utils.surrogate_key(['WORKFLOW_PROFILE','SOURCEID','SOURCE_ENTITY','TARGET_ENTITY','SOURCE_ACCOUNT','TARGET_ACCOUNT','SOURCE_IC','TARGET_IC','SOURCE_MAINUD1','TARGET_MAINUD1','SOURCE_MAINUD2','TARGET_MAINUD2','SOURCE_MAINUD3','TARGET_MAINUD3','FISCAL_PERIOD']) }} AS UNIQUE_KEY,
WORKFLOW_PROFILE,
SOURCEID,
SOURCE_DESC,
FISCAL_PERIOD,
SOURCE_ENTITY,
TARGET_ENTITY,
SOURCE_ACCOUNT,
TARGET_ACCOUNT,
SOURCE_IC,
TARGET_IC,
SOURCE_MAINUD1,
TARGET_MAINUD1,
SOURCE_MAINUD2,
TARGET_MAINUD2,
SOURCE_MAINUD3,
TARGET_MAINUD3,
MONTHLY_AMOUNT,
cast(CURRENT_DATE as timestamp_ntz) AS LOAD_DATE,
cast(CURRENT_DATE as timestamp_ntz) AS UPDATE_DATE
from Monthly_Amt_Balance_Sheet)

select '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,* from Final
