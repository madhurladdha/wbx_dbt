    {{
    config(
    materialized = 'view',
    )
}}

WITH old_dim AS 
        (
            SELECT * FROM {{source('EI_RDM','sls_wtx_promo_dim')}} where {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
        ),
        
converted_dim AS
(
    SELECT  
        promo_guid as promo_guid_old,
        {{ dbt_utils.surrogate_key(['source_system','promo_id']) }} as promo_guid,
        source_system,
        promo_id,
        promo_code,
        promo_desc,
        promo_group_id,
        promo_group_desc,
        promo_cat_id,
        promo_cat_desc,
        promo_tactic_id,
        promo_tactic_desc,
        promo_sub_tactic_id,
        promo_sub_tactic_desc,
        promo_sub_tactic_discount,
        promo_sub_tactic_isactive,
        promo_stat_id,
        promo_stat_desc,
        promo_phase_id,
        promo_phase_desc,
        trunc(promo_phase_length) as promo_phase_length ,
        promo_phase_effect_id,
        promo_phase_effect_desc,
        promo_phase_type_id,
        promo_phase_type_desc,
        promo_phase_type_unit,
        authorized_user_name,
        performance_start_dt,
        performance_end_dt,
        ship_start_dt,
        ship_end_dt,
        allowance_start_dt,
        allowance_end_dt,
        last_update_date,
        buy_in_start_dt,
        buy_in_end_dt,
        in_store_start_dt,
        in_store_end_dt,
        template_start_dt,
        template_end_dt,
        snapshot_date,
        update_date,
        feature,
        feature_desc,
        promo_mechanic_name
    FROM old_dim
)

select {{ dbt_utils.surrogate_key(['promo_guid','SNAPSHOT_DATE']) }} as unique_key,* from converted_dim