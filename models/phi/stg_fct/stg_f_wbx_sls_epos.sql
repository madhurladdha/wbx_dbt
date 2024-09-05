{{ config(tags=["sales", "epos","sls_epos"]) }}

with src_srpt_epos as (select * from {{ ref("src_srpt_epos") }})

select
    '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
    tratypcde as trade_type,
    to_char(lpad(comcde5d, 5, 0)) as source_item_identifier,
    cyrwk as calendar_week,
    sum(eposcases) as qty_ca
from src_srpt_epos
group by trade_type, to_char(lpad(comcde5d, 5, 0)), cyrwk
