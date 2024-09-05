{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('R_EI_SYSADM','adr_wtx_work_center')}} where {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

converted_dim as (
select 
work_center_code,
description,
source_business_unit_code,
company_code,
wc_category_code,
wc_category_desc,
source_system,
update_date
from old_dim
)

select * from converted_dim