select *
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'S' %}    
    from {{ ref("src_d365s_bom") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'D' %}
    from {{ ref("src_d365_bom") }}
{% endif %}
{% if env_var('DBT_SRC_D365_SCHEMA_FLAG') == 'A' %}
    from {{ ref("src_ax_bom") }}
{% endif %}