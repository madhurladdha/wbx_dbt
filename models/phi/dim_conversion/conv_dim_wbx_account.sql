{{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

/* For this conversion model for the ACCOUNT dimension, we require the following old and new fields:
    -source_business_unit_code, which is actually cost center
    -source_account_identifier, which is the unique code but for Weetabix is mapped from a RECID field.  Not the logical "Account" known to functional users.
    -source_object_id, which is mapped from MAINACCOUNT field for Weetabix.  This is the logical "Account" known to functional users.
    -source_company_code DOES NOT NEED an old new value as this is the 1 part of the unique key that will remain the same: WBX, IBE, etc.

    Specific to this Account dimension model, we are filtering from the old table so that only the relevant company values (source_company_code) are included in the AX history conversion 
    data set.  The applicable companies are ('WBX','RFL','IBE') even though there will be no IBE data.  This filter suppresses all the other companies 
    that are simply redundance and not used anywhere today regardless.
*/

with old_dim as (
    select *
    from {{ source('WBX_PROD','dim_wbx_account') }}
    where {{ env_var("DBT_PICK_FROM_CONV") }} = 'Y'
    and source_company_code in ('WBX','RFL')
),
/*added company_code filter to pick only wbx from old ax production as per reqirement"*/

plant_cross_ref as (select * from {{ ref("plant_d365_ref") }}),
account_cross_ref as (select * from {{ ref("account_d365_ref") }}),
/* Need the D365 mainaccount table to get the xref to the new recid which is becomes dim_wbx_account.source_account_identifier.
    This joins the D365 mainaccount w/ the xref so that we have the old/new values for source_account_identifier later.
*/
mainaccount as (
    select 
        ma.mainaccountid as source_object_id, 
        acr.ax as source_object_id_old,
        ma.recid as source_account_identifier, 
    from {{ref('src_mainaccount')}} ma
    join account_cross_ref acr
        on ma.mainaccountid = acr.d365
),


converted_dim as (
    select
        unique_key as unique_key_old,
        source_system,
        ax.source_concat_nat_key as source_concat_nat_key_old,
        (
            ax.source_company_code
            || '.'
            || nvl(plant_ref.d365, ax.source_business_unit_code)
            || '.'
            || cast(cast(substring(nvl(acc_ref.d365, ax.source_object_id),1,255) as number) as text(255))
        ) as source_concat_nat_key,
        ax.account_guid as account_guid_old,
        ma.source_account_identifier as source_account_identifier,  --Getting the new value from the recid from the D365 mainaccount table.
        ax.source_account_identifier as source_account_identifier_old,
        tagetik_account,
        ax.source_object_id as source_object_id_old,
        cast(cast(substring(nvl(acc_ref.d365, ax.source_object_id),1,255) as number) as text(255))  as source_object_id,
        source_subsidiary_id,
        source_company_code,
        ax.source_business_unit_code as source_business_unit_code_old,
        cast(substring(nvl(plant_ref.d365, ax.source_business_unit_code),1,255) as text(255))  as source_business_unit_code,
        account_type,
        account_description,
        account_level,
        entry_allowed_flag,
        load_date,
        date_updated,
        tagetik_cost_center,
        account_category,
        account_subcategory,
        stat_uom,
        consolidation_account
    from old_dim as ax 
    join mainaccount ma
        on upper(trim(ax.source_object_id)) = upper(trim(ma.source_object_id_old))
    left join
        account_cross_ref as acc_ref
        on upper(trim(ax.source_object_id)) = upper(trim(acc_ref.ax))
    left join
        plant_cross_ref as plant_ref
        on upper(trim(ax.source_business_unit_code)) = upper(trim(plant_ref.ax))
),

/* Generate the new account_guid since the the concat_nat_key includes the new Cost Center and Source Object Id.
*/
final as (
    select
        {{ dbt_utils.surrogate_key(['source_system','source_concat_nat_key']) }}
            as account_guid,
        *
    from converted_dim
),

/* Generate the new unique key since the account guid is newly generated too.
*/
gen_unique_key as (
select
    {{ dbt_utils.surrogate_key(['ACCOUNT_GUID']) }} as unique_key,
    *
from final),

/* Logic in this CTE added to avoid duplication in conv_dim_wbx_account due to 1-to-M xref data for source_object_id.
    This scenario should not occur in the provided data, but has during testing so this code avoids unwanted duplication.
    The Parition By clause to remove duplicates as source new "SOURCE_OBJECT_ID" has one to many relationship with old "source_object_id" in account seed file.
*/
gen_unique_key_resolve as
(
    select * from (select
    row_number()
        over (
            partition by unique_key_old  -- Here we have to use the old unique value as that is the potentially duplicated key
            order by source_object_id desc
        )
        as rownum,
        *
from gen_unique_key)
where rownum = 1
    )

select
        cast (substr(unique_key,1,255) as varchar2(255)) as unique_key,
        cast (substr(source_system,1,255) as varchar2(255)) as source_system,
        cast (substr(source_concat_nat_key,1,255) as varchar2(255)) as source_concat_nat_key,
        cast (account_guid_old as varchar2(255)) as account_guid_old,
        cast (substr(account_guid,1,255) as varchar2(255)) as account_guid,
        cast (substr(source_account_identifier_old,1,255) as varchar2(255)) as source_account_identifier_old,
        cast (substr(source_account_identifier,1,255) as varchar2(255)) as source_account_identifier,
        cast (substr(tagetik_account,1,255) as varchar2(255)) as tagetik_account,
        cast (substr(source_object_id_old,1,255) as varchar2(255)) as source_object_id_old,
        cast (substr(source_object_id,1,255) as varchar2(255)) as source_object_id,
        cast (substr(source_subsidiary_id,1,255) as varchar2(255)) as source_subsidiary_id,
        cast (substr(source_company_code,1,255) as varchar2(255)) as source_company_code,
        cast (substr(source_business_unit_code_old,1,255) as varchar2(255)) as source_business_unit_code_old,
        cast (substr(source_business_unit_code,1,255) as varchar2(255)) as source_business_unit_code,
        cast (substr(account_type,1,255) as varchar2(255)) as account_type,
        cast (substr(account_description,1,255) as varchar2(255)) as account_description,
        cast (substr(account_level,1,255) as varchar2(255)) as account_level,
        cast (substr(entry_allowed_flag,1,255) as varchar2(255)) as entry_allowed_flag,
        cast (load_date as timestamp_ntz(9)) as load_date,
        cast (date_updated as timestamp_ntz(9)) as date_updated,
        cast (substr(tagetik_cost_center,1,60) as varchar2(60)) as tagetik_cost_center,
        cast (substr(account_category,1,255) as varchar2(255)) as account_category,
        cast (substr(account_subcategory,1,255) as varchar2(255)) as account_subcategory,
        cast (substr(stat_uom,1,255) as varchar2(255)) as stat_uom,
        cast (substr(consolidation_account,1,30) as varchar2(30)) as consolidation_account
    from gen_unique_key_resolve
