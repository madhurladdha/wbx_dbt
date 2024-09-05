{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["procurement", "usage","prc_usage"],
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

with old_table as
(
    select * from {{ref('conv_wbx_prc_usage_fact')}}  
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),

base_fct  as (
    select * from {{ref('int_f_wbx_prc_usage')}}
    {% if check_table_exists( this.schema, this.table ) == 'True' %}
     limit {{env_var('DBT_NO_LIMIT')}}
    {% else %} limit {{env_var('DBT_LIMIT')}}
    {% endif %}
),
old_model as
(
    select
    cast(substring(source_system,1,255) as text(255) )              as source_system,
    cast(substring(source_item_identifier,1,255) as text(255) )     as source_item_identifier,
      cast({{ dbt_utils.surrogate_key(
    ["source_system","source_item_identifier"]) }} as text(255) )   as item_guid  ,
    cast(substring(source_business_unit_code,1,24) as text(24) )    as source_business_unit_code  ,
    cast({{ dbt_utils.surrogate_key(["source_system",
    "source_business_unit_code","'PLANT_DC'"]) }} as text(255) )    as business_unit_address_guid  ,
    cast(fiscal_period_number as number(38,0) )                     as fiscal_period_number,
    cast(substring(input_uom,1,6) as text(6) )                      as input_uom,
   -- cast(substring(gl_cat_c,1,6) as text(6) )                       as gl_cat_c, 
    cast(ending_inventory_qty as number(38,10) )                    as ending_inventory_qty,
    cast(beginning_inventory_qty as number(38,10) )                 as beginning_inventory_qty,
    cast(receipt_qty as number(38,10) )                             as receipt_qty,
    cast(transfer_in_qty as number(38,10) )                         as transfer_in_qty,
    cast(transfer_out_qty as number(38,10) )                        as transfer_out_qty,
    cast(transfer_in_intercompany_qty as number(38,10) )            as transfer_in_intercompany_qty,
    cast(transfer_out_intercompany_qty as number(38,10) )           as transfer_out_intercompany_qty,
    cast(transfer_in_intracompany_qty as number(38,10) )            as transfer_in_intracompany_qty,
    cast(transfer_out_intracompany_qty as number(38,10) )           as transfer_out_intracompany_qty,
    cast(current_timestamp() as timestamp_ntz(9) )                  as load_date,
    cast(current_timestamp() as timestamp_ntz(9) )                  as update_date,
    cast(unique_key as text(255) )                                  as unique_key
from old_table
),
snpt_fact as (
    select 
    cast(substring(source_system,1,255) as text(255) )              as source_system,
    cast(substring(source_item_identifier,1,255) as text(255) )     as source_item_identifier,
    cast(item_guid as text(255) )                                   as item_guid,
    cast(substring(source_business_unit_code,1,255) as text(255) )  as source_business_unit_code,
    cast(business_unit_address_guid as text(255) )                  as business_unit_address_guid,
    cast(fiscal_period_number as number(38,0) )                     as fiscal_period_number,
    cast(substring(input_uom,1,6) as text(6) )                      as input_uom,
   -- cast(substring(gl_cat_c,1,6) as text(6) )                       as gl_cat_c, 
    cast(ending_inventory_qty as number(38,10) )                    as ending_inventory_qty,
    cast(beginning_inventory_qty as number(38,10) )                 as beginning_inventory_qty,
    cast(receipt_qty as number(38,10) )                             as receipt_qty,
    cast(transfer_in_qty as number(38,10) )                         as transfer_in_qty,
    cast(transfer_out_qty as number(38,10) )                        as transfer_out_qty,
    cast(transfer_in_intercompany_qty as number(38,10) )            as transfer_in_intercompany_qty,
    cast(transfer_out_intercompany_qty as number(38,10) )           as transfer_out_intercompany_qty,
    cast(transfer_in_intracompany_qty as number(38,10) )            as transfer_in_intracompany_qty,
    cast(transfer_out_intracompany_qty as number(38,10) )           as transfer_out_intracompany_qty,
    cast(current_timestamp() as timestamp_ntz(9) )                  as load_date,
    cast(current_timestamp() as timestamp_ntz(9) )                  as update_date,
    cast(unique_key as text(255) )                                  as unique_key
from base_fct bf
    
)

select * from snpt_fact
union
select * from old_model