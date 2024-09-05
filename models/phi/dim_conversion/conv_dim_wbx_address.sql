{{
    config(
    materialized = env_var("DBT_MAT_TABLE"),
    tags=["ax_hist_dim"]
    )
}}

with conv_addr as (

    select * from {{ source('WBX_PROD','dim_wbx_address') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y'

),

old_plant as (
    select * from {{ref('conv_dim_wbx_plant_dc')}}
),

get_Company as (
   select distinct GENERIC_ADDRESS_TYPE,SOURCE_SYSTEM_ADDRESS_NUMBER,COMPANY_CODE from {{ source('WBX_PROD','dim_wbx_customer') }} where {{env_var("DBT_PICK_FROM_CONV")}}='Y'
),

renamed as (

    select
        B.plantdc_address_guid_new,
        A.generic_Address_type as generic_Address_type,
        A.address_guid as address_guid_old,
        A.source_system as source_system,
        case when A.generic_Address_type='PLANT_DC' then b.Source_business_unit_code_new else A.source_system_address_number  end as source_system_address_number,
        address_type,
        address_type_description,
        long_address_number,
        source_name,
        department_name,
        case when A.generic_Address_type='PLANT_DC' then b.Source_business_unit_code_new else A.source_business_unit_code end as source_business_unit_code,
        business_unit,
        tax_type,
        tax_number,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        postal_code,
        city,
        county,
        state_province_code,
        state_province,
        country_code,
        country,
        contact_1_first_name,
        contact_1_last_name,
        contact_1_primary_phone_type,
        contact_1_primary_phone_number,
        contact_1_department_name,
        contact_1_email,
        contact_2_first_name,
        contact_2_last_name,
        contact_2_primary_phone_type,
        contact_2_primary_phone_number,
        contact_2_department_name,
        A.date_inserted as date_inserted,
        A.date_updated as date_updated,
        required_1099,
        active_indicator,
        source_payee_id,
        case when A.generic_Address_type='PLANT_DC' then B.plantdc_address_guid_new else a.payee_guid end  as payee_guid,
        payee_guid as payee_guid_old
    from conv_addr A
    left join old_plant B on A.source_system_address_number=B.source_business_unit_code
    and A.generic_Address_type='PLANT_DC'

),

final as(
select 
nvl(b.company_code,'WBX') as Company_code,         /*we cannot hardcode the company code to wbx as if the customer is of RFL then it would create new row*/
{{ dbt_utils.surrogate_key(['SOURCE_SYSTEM',"nvl(b.company_code,'WBX')"]) }} AS COMPANY_CODE_GUID,
case when A.generic_Address_type='PLANT_DC' then A.plantdc_address_guid_new
             when A.generic_Address_type in ('CUSTOMER_MAIN','SUPPLIER') then 
              {{ dbt_utils.surrogate_key(['a.source_system','a.source_system_address_number','a.GENERIC_ADDRESS_TYPE',"nvl(b.company_code,'WBX')"]) }}
         else a.address_guid_old end as address_guid,
/* For plant we will get the GUID from Plant conversion model for Customer and Supplier we have added GUID based on Company code so
generating the GUID again*/
a.* 
from renamed a left join get_Company b on a.SOURCE_SYSTEM_ADDRESS_NUMBER=b.SOURCE_SYSTEM_ADDRESS_NUMBER
and a.generic_Address_type=b.generic_Address_type
 )
 
 
select {{ dbt_utils.surrogate_key(['ADDRESS_GUID']) }} as UNIQUE_KEY,* from Final