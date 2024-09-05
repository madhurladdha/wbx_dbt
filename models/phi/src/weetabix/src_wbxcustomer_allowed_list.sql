/* This table was not replicated for AX.  So if the flag is set for AX, then will simply default to the Synapse version.
*/

select *
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'S' %}    
    from {{ ref("src_d365s_wbxcustomerallowedlist") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'D' %}
    from {{ ref("src_d365_wbxcustomer_allowed_list") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'A' %}
    from {{ ref("src_d365s_wbxcustomerallowedlist") }}
{% endif %}