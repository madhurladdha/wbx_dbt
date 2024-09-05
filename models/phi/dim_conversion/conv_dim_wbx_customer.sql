{{
    config(
    materialized =env_var('DBT_MAT_TABLE'),
    tags=["ax_hist_dim"]
    )
}}

WITH old_dim AS 
        (
            SELECT  * FROM {{source('WBX_PROD','dim_wbx_customer')}} WHERE  {{env_var("DBT_PICK_FROM_CONV")}}='Y'
        ),

converted_dim AS
(
    SELECT
    UNIQUE_KEY,
        GENERIC_ADDRESS_TYPE,
        {{ dbt_utils.surrogate_key(['source_system','source_system_address_number','GENERIC_ADDRESS_TYPE','company_code']) }} AS CUSTOMER_ADDRESS_NUMBER_GUID,
        /* Generating the GUID again as now we are adding company code as part of key for customer */
        customer_address_number_guid_old,
        company_address_guid,
        company_address_guid_old,
        source_system,
        source_system_address_number,
        company_code,
        company_name,
        customer_name,
        customer_type,
        bill_to,
        bill_address_type,
        bill_address_type_description,
        freight_handling_code,
        payment_terms_code,
        payment_terms_description,
        csr_address_number,
        csr_name,
        credit_limit,
        unified_customer,
        shipping_terms,
        date_inserted,
        date_updated,
        source_customer_code,
        customer_group,
        customer_group_name,
        legacy_customer_number,
        currency_code,
        transport_mode,
        bill_name
    FROM old_dim
)

select * from converted_dim
