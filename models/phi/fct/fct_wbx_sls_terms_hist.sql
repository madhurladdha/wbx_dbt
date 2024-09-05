-- depends_on: {{ ref('dim_wbx_fc_snapshot') }}
-- depends_on: {{ ref('fct_wbx_sls_terms') }}


{{
  config( 
    materialized=env_var('DBT_MAT_INCREMENTAL'), 
    tags=["sales_terms_hist","sales_archive"],
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    unique_key='snapshot_date', 
    on_schema_change='sync_all_columns', 
    incremental_strategy='merge',
    full_refresh=false,
    post_hook=
        """
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
        DELETE FROM {{ref('fct_wbx_sls_terms')}} WHERE SNAPSHOT_DATE NOT IN (SELECT DISTINCT SNAPSHOT_DATE FROM {{ref('dim_wbx_fc_snapshot')}} ) 
        OR SNAPSHOT_DATE < DATEADD(MONTH,-1,CURRENT_DATE) 
		{% endif %}  
        """
    )
}}


with hist_fct as
(
    select * from {{ ref('conv_fct_wbx_sls_terms_hist') }} 
    {% if check_table_exists( this.schema, this.table ) == 'False' %}
     limit {{env_var('DBT_NO_LIMIT')}} ----------Variable DBT_NO_LIMIT variable is set TO NULL to load everything from conv model if effective currency model is not present.
    {% else %} limit {{env_var('DBT_LIMIT')}}-----Variable DBT_LIMIT variable is set to 0 to load nothing if effective_currency table exist

{% endif %}

),

base_fct  as (
    select 
        source_system,
        cust_idx,
        plan_source_customer_code,
        customer_address_number_guid,
        sku_idx,
        source_item_identifier,
        item_guid,
        calendar_date,
        snapshot_date,
        term_code,
        term_desc,
        term_create_datetime,
        term_created_by,
        rsa_perc,
        lump_sum,
        perc_invoiced_sales,
        perc_gross_sales,
        early_settlement_perc,
        edlp_perc,
        edlp_case_rate,
        long_term_promo,
        rsi_perc,
        fixed_annual_payment,
        direct_shopper_marketing,
        other_direct_payment,
        other_direct_perc,
        category_payment,
        indirect_shopper_marketing,
        other_indirect_payment,
        other_indirect_perc,
        field_marketing,
        consumer_spend,
        term_start_date,
        term_end_date,
        status_code,
        status_name,
        status_verb,
        impact_option_code,
        impact_option_name,
        impact_code,
        impact_name,
        impact_option_valvol_percent,
        impact_option_lump_sum_flag,
        impact_option_value,
        impact_option_fin_impact_estimate,
        unique_key 
        from {{ ref ('fct_wbx_sls_terms') }}
        {% if check_table_exists( this.schema, this.table ) == 'True' %}
           limit {{env_var('DBT_NO_LIMIT')}}
            {% else %} limit {{env_var('DBT_LIMIT')}}
            {% endif %}
    ),


old as (
select * from hist_fct
),


new as
(
  {% if check_table_exists( this.schema, this.table ) == 'True' %}
    SELECT * FROM  base_fct WHERE SNAPSHOT_DATE > (SELECT MAX(SNAPSHOT_DATE) FROM {{this}})
    {% else %}
    select * from base_fct
    {% endif %}
),


final as(
    select * from old
    union
    select * from new
)


select * from final
