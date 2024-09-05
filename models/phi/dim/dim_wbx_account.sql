{{
    config(
    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    unique_key = 'unique_key',
    on_schema_change='sync_all_columns'
    )
}}

WITH old_account_dim AS

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

        /*Adding partition by clause to remove duplicates as source new "SOURCE_OBJECT_ID" has one to many relationship with old "source_object_id" in account seed file */
        
accounts AS 
        (
            SELECT * FROM {{ref('int_d_wbx_account')}}
        ),

cogs_swap AS 
        (
            SELECT * FROM {{ref('src_ref_fin_cogs_swap')}}
        ),

hier_cc AS 
        (
            SELECT * FROM {{ref('xref_wbx_hierarchy')}} where hier_name = 'CONSOLIDATION_COSTCENTER'
        ),

fin_consolidation AS 
        (
        SELECT * FROM (
        select UID,TARGET_ACCOUNT,TARGET_MAINUD1,ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM,SOURCE_CONCAT_NAT_KEY,UID ORDER BY 1) rowNum  from 
        {{ref('int_d_wbx_account_consolidation')}}
        where SOURCE_SYSTEM is not null
        and SOURCE_CONCAT_NAT_KEY is not null
        and uid is not null
        group by UID,TARGET_ACCOUNT,TARGET_MAINUD1,SOURCE_SYSTEM,SOURCE_CONCAT_NAT_KEY) where rowNum=1
        ),
    
hier_cc_cogs AS 
        (
            SELECT hier_cc.leaf_node, cogs_swap.account_prepend
            FROM hier_cc
            LEFT JOIN cogs_swap
                ON hier_cc.node_5 like cogs_swap.target_cc||'%'
        ),  

accounts_with_trim AS
    (
    SELECT accounts.source_system as source_system,
        ltrim(rtrim(accounts.source_concat_nat_key)) AS source_concat_nat_key,
        NULL AS account_guid_old,
        {{ dbt_utils.surrogate_key(['accounts.source_system','accounts.source_concat_nat_key']) }} AS ACCOUNT_GUID,
        ltrim(rtrim(upper(accounts.source_account_identifier))) AS source_account_identifier,
        accounts.tagetik_account as tagetik_account,
        ltrim(rtrim(accounts.source_object_id)) AS source_object_id,
        ltrim(rtrim(upper(accounts.source_subsidiary_id))) AS source_subsidiary_id,
        ltrim(rtrim(upper(accounts.source_company_code))) AS source_company_code,
        ltrim(rtrim(upper(accounts.source_business_unit_code))) AS source_business_unit_code,
        ltrim(rtrim(accounts.account_type)) as account_type,
        ltrim(rtrim(accounts.account_description)) as account_description,
        ltrim(rtrim(upper(accounts.account_level))) AS account_level,
        ltrim(rtrim(upper(accounts.entry_allowed_flag))) AS entry_allowed_flag,
        account_category,
        account_subcategory,
        null as stat_uom,
        NULL as consolidation_account,
        NULL as tagetik_cost_center
    FROM accounts
   ),

accounts_with_tagetik AS
    (
    SELECT accounts_with_trim.source_system as source_system,
        accounts_with_trim.source_concat_nat_key AS source_concat_nat_key,
        accounts_with_trim.account_guid_old,
        accounts_with_trim.account_guid,
        accounts_with_trim.source_account_identifier,
        nvl(fin_consolidation.target_account,accounts_with_trim.tagetik_account) as tagetik_account,
        accounts_with_trim.source_object_id AS source_object_id,
        accounts_with_trim.source_subsidiary_id AS source_subsidiary_id,
        accounts_with_trim.source_company_code AS source_company_code,
        accounts_with_trim.source_business_unit_code,
        accounts_with_trim.account_type,
        accounts_with_trim.account_description,
        accounts_with_trim.account_level,
        accounts_with_trim.entry_allowed_flag,
        nvl(fin_consolidation.target_mainud1,accounts_with_trim.tagetik_cost_center) as tagetik_cost_center,
        accounts_with_trim.account_category,
        accounts_with_trim.account_subcategory,
        stat_uom,
        consolidation_account,
        CURRENT_TIMESTAMP AS LOAD_DATE,
        CURRENT_TIMESTAMP AS DATE_UPDATED
    FROM accounts_with_trim
    LEFT JOIN fin_consolidation 
        ON accounts_with_trim.account_guid = fin_consolidation.uid
   ),

   accounts_with_consolidation AS
    (
    SELECT {{ dbt_utils.surrogate_key(['accounts_with_tagetik.account_guid']) }} as unique_key,
        accounts_with_tagetik.source_system,
        accounts_with_tagetik.source_concat_nat_key,
        accounts_with_tagetik.account_guid_old,
        accounts_with_tagetik.account_guid,
        accounts_with_tagetik.source_account_identifier,
        accounts_with_tagetik.tagetik_account,
        accounts_with_tagetik.source_object_id,
        accounts_with_tagetik.source_subsidiary_id,
        accounts_with_tagetik.source_company_code,
        accounts_with_tagetik.source_business_unit_code,
        accounts_with_tagetik.account_type,
        accounts_with_tagetik.account_description,
        accounts_with_tagetik.account_level,
        accounts_with_tagetik.entry_allowed_flag,
        accounts_with_tagetik.tagetik_cost_center,
        accounts_with_tagetik.account_category,
        accounts_with_tagetik.account_subcategory,
        accounts_with_tagetik.stat_uom,
       case when accounts_with_tagetik.tagetik_account ='8675309' Then Null
            when accounts_with_tagetik.tagetik_account <>'8675309' and ltrim(hier_cc_cogs.ACCOUNT_PREPEND||accounts_with_tagetik.tagetik_account, '_') is null
            then accounts_with_tagetik.tagetik_account 
             else ltrim(hier_cc_cogs.ACCOUNT_PREPEND||accounts_with_tagetik.tagetik_account, '_') 
             end as consolidation_account,
        CURRENT_TIMESTAMP AS LOAD_DATE,
        CURRENT_TIMESTAMP AS DATE_UPDATED
    FROM accounts_with_tagetik
    LEFT JOIN hier_cc_cogs 
        ON accounts_with_tagetik.tagetik_cost_center = hier_cc_cogs.leaf_node
   ),

   new_dim as
	(
		select
		a.unique_key,
        a.source_system,
        a.source_concat_nat_key,
        b.account_guid_old,
        a.account_guid,
        a.source_account_identifier,
        case when a.tagetik_account='8675309'  and a.tagetik_account=b.tagetik_account then a.tagetik_account
             when a.tagetik_account is not null and a.tagetik_account<>'8675309' then a.tagetik_account
             else b.tagetik_account end as tagetik_account,
        a.source_object_id,
        a.source_subsidiary_id,
        a.source_company_code,
        a.source_business_unit_code,
        a.account_type,
        a.account_description,
        a.account_level,
        a.entry_allowed_flag,
        case when a.tagetik_account='8675309' and a.tagetik_account=b.tagetik_account then a.tagetik_cost_center
             when a.tagetik_account is not null and a.tagetik_account<>'8675309' then a.tagetik_cost_center
             else b.tagetik_cost_center end as tagetik_cost_center,
        a.account_category,
        a.account_subcategory,
        a.stat_uom,
        nvl(a.consolidation_account,b.consolidation_account) as consolidation_account,
        a.load_date,
        a.date_updated
		from accounts_with_consolidation a
		left join old_account_dim b
		on a.account_guid=b.account_guid
	),
	
/* Reason for above logic
Tagetik Account and Tagetik Cost Center coming from v_fin_consolidation_src.
We have converted same in dbt -int_d_8ave_account_consolidation model.
There are some source_account_identifier for which there is no presence in view
but  Tagetik Account and Tagetik Cost Center having values in final account dim.
These are never updated even if not available in consolidation view (v_fin_consolidation_src). 
Applied this logic to bring all those missing data into dbt model. 
*/



	old_dim as
	(
		select
		a.unique_key,
        a.source_system,
        a.source_concat_nat_key,
        a.account_guid_old,
        a.account_guid,
        a.source_account_identifier,
        a.tagetik_account,
        a.source_object_id,
        a.source_subsidiary_id,
        a.source_company_code,
        a.source_business_unit_code,
        a.account_type,
        a.account_description,
        a.account_level,
        a.entry_allowed_flag,
        a.tagetik_cost_center,
        a.account_category,
        a.account_subcategory,
        a.stat_uom,
        a.consolidation_account,
        a.load_date,
        a.date_updated
		from old_account_dim a
		left join accounts_with_consolidation b
		on a.account_guid=b.account_guid
		where b.unique_key is null
	),
	
 blending AS
   (
      select * from new_dim 
	  union 
	  select * from old_dim
   ),

accounts_with_cast as
   (
    select 
        cast (substr(unique_key,1,255) as varchar2(255)) as unique_key,
        cast (substr(source_system,1,255) as varchar2(255)) as source_system,
        cast (substr(source_concat_nat_key,1,255) as varchar2(255)) as source_concat_nat_key,
        cast (account_guid_old as varchar2(255)) as account_guid_old,
        cast (substr(account_guid,1,255) as varchar2(255)) as account_guid,
        cast (substr(source_account_identifier,1,255) as varchar2(255)) as source_account_identifier,
        cast (substr(tagetik_account,1,255) as varchar2(255)) as tagetik_account,
        cast (substr(source_object_id,1,255) as varchar2(255)) as source_object_id,
        cast (substr(source_subsidiary_id,1,255) as varchar2(255)) as source_subsidiary_id,
        cast (substr(source_company_code,1,255) as varchar2(255)) as source_company_code,
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
    from blending
   )

   select * from accounts_with_cast