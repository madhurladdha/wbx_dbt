{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["item_cost","prc_item_cost","procurement"],
    unique_key='fiscal_period_number', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='delete+insert',
    full_refresh=false,
    )
}}

/* Approach Used: Static Snapshot w/ Historical Conversion
    The approach used for this table is a Snapshot approach but also requires historical conversion from the old IICS data sets.
    Full details can be found in applicable documentation, but the highlights are provided here.
    1) References the old "conversion" or IICS data set for all snapshots up to the migration date.
    2) Environment variables used to drive the filtering so that the IICS data set is only pulled in on the initial run of the model in a new db/env.
    3) Same variables are used to drive filtering on the new (go-forward) data set
    4) End result should be that all old snapshots are captured and then this dbt model appends each new snapshot/version date to the data set in the dbt model.
    Other Design features:
    1) Model should NEVER be allowed to full-refresh.  This could wipe out all history.
    2) Model is incremental with unique_key = version date.  This ensures that past version dates are never deleted and re-runs on the same day will simply delete for
        the given version date and reload.
*/




with cte_date_src as (select * from {{ ref('src_dim_date')}} ),

old_table as
(
    select * from {{ref('conv_wbx_prc_itmcst_month_dim')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),
/*
This is required for the roll-forward logic so the selective filtering is applied in a later step.
*/
base_fct_main as (
    select * from {{ ref ('int_f_wbx_prc_itmcst_month_dim') }}
),
base_fct  as (
    select * from base_fct_main
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
/*
Need to look back 2 months from the current date.  So if we are in Mar, then need the previous results from Jan to pull forward to Feb.
*/
cte_dim_date_range as 
(
  select 
        distinct fiscal_year_period_no
    from cte_date_src 
    where calendar_date between  add_months(current_date, -{{ env_var("DBT_PROCUREMENT_LOOKBACK") }}) 
    and   add_months(current_date,-1) 
),
cte_last_mnth_frm_fct as --this cte is to take int - 1 month scope, if int model is processing last 6 fiscal_period. this cte will pick 7th fiscal_period
(
    select max(fiscal_year_period_no) fiscal_year_period_no from cte_date_src
    where fiscal_year_period_no < (select min(fiscal_period_number) from base_fct)
),
cte_dt as ( --select all the FISCAL_YEAR_PERIOD_NO in scope of int table
    select 
        distinct fiscal_year_period_no 
    from cte_dim_date_range
  union 
  select fiscal_year_period_no from cte_last_mnth_frm_fct
  ),

/*
This is the core of the roll-forward logic.  We must pull from the item cost model itself from the prev period/month for any items
for which there is no supporting transactional data from po_receipt.  This chunk of code only runs if the table already exists.
Since we are always processing the previous month, compared to current date, for procurement.  In this context, the previous period/month means 2 months back.
*/
{% if check_table_exists( this.schema, this.table ) == 'True' %}
cte_union_all as
(
    select        
        cast(substring(a.source_system,1,255) as text(255) )                 as source_system,
        cast(substring(a.source_item_identifier,1,60) as text(60) )          as source_item_identifier  ,
        cast({{ dbt_utils.surrogate_key(["a.source_system","a.source_item_identifier"]) }} as text(255) ) as item_guid  ,
        cast(substring(a.source_business_unit_code,1,24) as text(24) )       as source_business_unit_code  ,
        cast({{ dbt_utils.surrogate_key(["a.source_system","a.source_business_unit_code","'PLANT_DC'"]) }} as text(255) ) as business_unit_address_guid,
        cast(FISCAL_PERIOD_NUMBER as number(38,0) )   as fiscal_period_number,   --For roll-forward, moving this up last mont
        cast(substring(a.input_uom,1,6) as text(6) )                         as input_uom,
        cast(substring(a.cost_method,1,6) as text(6) )                       as cost_method,
        cast(a.phi_unit_cost as number(14,4) )                               as phi_unit_cost,
        cast(substring(a.phi_currency_code,1,6) as text(6) )                 as phi_currency_code,
        cast(cost_rollfwd_flag as text(2) )                                                as cost_rollfwd_flag,
        cast(a.source_exchange_rate as number(14,7) )                        as source_exchange_rate,
        cast(a.base_unit_cost as number(14,4) )                              as base_unit_cost  ,
        cast(substring(a.base_currency_code,1,6) as text(6) )                as base_currency_code  ,
        cast(a.phi_conv_rt as number(14,7) )                                 as phi_conv_rt,
        cast(a.unique_key as text(255) )                                     as unique_key 
    from {{this}} a
    join cte_last_mnth_frm_fct
        on a.fiscal_period_number = cte_last_mnth_frm_fct.fiscal_year_period_no
    union all
    select        
        cast(substring(a.source_system,1,255) as text(255) )                 as source_system,
        cast(substring(a.source_item_identifier,1,60) as text(60) )          as source_item_identifier  ,
        cast({{ dbt_utils.surrogate_key(["a.source_system","a.source_item_identifier"]) }} as text(255) ) as item_guid  ,
        cast(substring(a.source_business_unit_code,1,24) as text(24) )       as source_business_unit_code  ,
        cast({{ dbt_utils.surrogate_key(["a.source_system","a.source_business_unit_code","'PLANT_DC'"]) }} as text(255) ) as business_unit_address_guid,
        cast(FISCAL_PERIOD_NUMBER as number(38,0) )   as fiscal_period_number,   --For roll-forward, moving this up last mont
        cast(substring(a.input_uom,1,6) as text(6) )                         as input_uom,
        cast(substring(a.cost_method,1,6) as text(6) )                       as cost_method,
        cast(a.phi_unit_cost as number(14,4) )                               as phi_unit_cost,
        cast(substring(a.phi_currency_code,1,6) as text(6) )                 as phi_currency_code,
        cast(cost_rollfwd_flag as text(2) )                                                as cost_rollfwd_flag,
        cast(a.source_exchange_rate as number(14,7) )                        as source_exchange_rate,
        cast(a.base_unit_cost as number(14,4) )                              as base_unit_cost  ,
        cast(substring(a.base_currency_code,1,6) as text(6) )                as base_currency_code  ,
        cast(a.phi_conv_rt as number(14,7) )                                 as phi_conv_rt,
        cast(a.unique_key as text(255) )                                     as unique_key
    from base_fct a
    /* Only take the rows where there is no row in the base_fct.*/
),
cte_distinct as ( --take distinct combination of items based on pk
    select 
        distinct source_system, 
        source_item_identifier,
        item_guid, 
        source_business_unit_code,
        business_unit_address_guid,
        cost_method
    from cte_union_all 
  ),
  all_item_fiscal_period as --take only those records which are in scope(int model+last one month) model for exploding in fiscal year
(      
        select 
            * 
        from cte_dt a 
        cross join cte_distinct t1 
        ORDER BY FISCAL_YEAR_PERIOD_NO
),
cte_all as 
(
    select 
        aifp.source_system, 
        aifp.source_item_identifier,
        aifp.item_guid,
        aifp.source_business_unit_code,
        aifp.business_unit_address_guid,
        aifp.fiscal_year_period_no as fiscal_period_number,
        cf.input_uom, 
        aifp.cost_method,
        cf.phi_unit_cost as unit_cost  , 
        cf.phi_currency_code as currency_code ,
        cf.cost_rollfwd_flag,
        cf.source_exchange_rate,
        cf.base_unit_cost,
        cf.base_currency_code,
        cf.phi_conv_rt
    from all_item_fiscal_period aifp 
    left join cte_union_all cf
    on aifp.fiscal_year_period_no=cf.fiscal_period_number
    and aifp.source_system=cf.source_system 
    and aifp.source_item_identifier=cf.source_item_identifier 
    and aifp.source_business_unit_code=cf.source_business_unit_code order by fiscal_year_period_no
),
cte_rollforward_int as 
(
  select
        source_system,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        fiscal_period_number,
        input_uom,
        cost_method,
        unit_cost,
        currency_code,
        cost_rollfwd_flag,
        source_exchange_rate,
        base_unit_cost,
        base_currency_code,
        phi_conv_rt,
        sum(case when input_uom is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_input_uom,
        sum(case when unit_cost is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_unit_cost,
        sum(case when currency_code is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_currency_code,
        sum(case when source_exchange_rate is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_source_exchange_rate,
        sum(case when base_unit_cost is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_base_unit_cost,
        sum(case when base_currency_code is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_base_currency_code,
        sum(case when phi_conv_rt is null then 0 else 1 end) 
        over(partition by source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid order by fiscal_period_number asc) as nf_phi_conv_rt
     from cte_all
),
cte_rollfwd as 
(
    select 
        source_system,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        fiscal_period_number,
        first_value(t.input_uom) 
                over(partition by t.nf_input_uom,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid 
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as input_uom,
                    cost_method,  
        first_value(t.unit_cost) 
                over(partition by t.nf_unit_cost ,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as unit_cost,
        first_value(t.currency_code) 
                over(partition by t.nf_currency_code,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as currency_code,
                    cost_rollfwd_flag,
        first_value(t.source_exchange_rate) 
                over(partition by t.nf_source_exchange_rate,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as source_exchange_rate,
        first_value(t.base_unit_cost) 
                over(partition by t.nf_base_unit_cost,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as base_unit_cost,
        first_value(t.base_currency_code) 
                over(partition by t.nf_base_currency_code,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as base_currency_code,
        first_value(t.phi_conv_rt) 
                over(partition by t.nf_phi_conv_rt,source_item_identifier,item_guid,source_business_unit_code,business_unit_address_guid
                    order by t.fiscal_period_number 
                    rows between unbounded preceding and current row) as phi_conv_rt
        from cte_rollforward_int t
),
cte_key_filter as 
(
    select 
        distinct 
        source_system,
        source_item_identifier,
        item_guid,
        source_business_unit_code,
        business_unit_address_guid,
        fiscal_period_number,
        input_uom,
        cost_method,
        unit_cost,
        currency_code,
        nvl(cost_rollfwd_flag,'Y') as cost_rollfwd_flag ,
        source_exchange_rate,
        base_unit_cost,
        base_currency_code,
        phi_conv_rt,
        {{ dbt_utils.surrogate_key(
            ["source_system",
            "source_item_identifier",
            "source_business_unit_code",
            "fiscal_period_number",
            "cost_method"]) }} as unique_key
    from cte_rollfwd join cte_dim_date_range
    on cte_rollfwd.fiscal_period_number=fiscal_year_period_no
    where unit_cost is not null
),
{% endif %}


old_model as
(
    select             
        cast(substring(o.source_system,1,255) as text(255) )           as source_system  ,
        cast(substring(o.source_item_identifier,1,60) as text(60) )    as source_item_identifier  ,
        cast({{ dbt_utils.surrogate_key(["source_system","source_item_identifier"]) }} as text(255) )  as item_guid  ,
        cast(substring(o.source_business_unit_code,1,24) as text(24) ) as source_business_unit_code  ,
        cast({{ dbt_utils.surrogate_key(["source_system","source_business_unit_code","'PLANT_DC'"]) }} as text(255) ) as business_unit_address_guid  ,
        cast(o.fiscal_period_number as number(38,0) )                  as fiscal_period_number  ,
        cast(substring(o.input_uom,1,6) as text(6) )                   as input_uom  ,
        cast(substring(o.cost_method,1,6) as text(6) )                 as cost_method  ,
        cast(o.phi_unit_cost as number(14,4) )                         as phi_unit_cost  ,
        cast(substring(o.phi_currency_code,1,6) as text(6) )           as phi_currency_code  ,
        cast(substring(o.cost_rollfwd_flag,1,1) as text(1) )           as cost_rollfwd_flag  ,
        cast(o.source_exchange_rate as number(14,7) )                  as source_exchange_rate  ,
        cast(o.base_unit_cost as number(14,4) )                        as base_unit_cost  ,
        cast(substring(o.base_currency_code,1,6) as text(6) )          as base_currency_code  ,
        cast(o.phi_conv_rt as number(14,7) )                           as phi_conv_rt,
        cast(o.unique_key as text(255) )                               as unique_key
    from old_table o
)



select * from old_model
{% if check_table_exists( this.schema, this.table ) == 'True' %}
union all
select * from cte_key_filter
{% endif %}