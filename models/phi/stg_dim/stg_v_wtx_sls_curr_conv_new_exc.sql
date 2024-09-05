with CURR_OPT as (
    select * from {{ ref('src_exc_dim_currency_exchange_options')}}
),
CURR_RATE as (
    select * from {{ ref('src_exc_fact_currency_exchange_rate')}}
),
Dim_Currency as (
    select * from {{ ref('src_exc_dim_currency')}}
),
final as (
    select 
        curr_to.currency_code as from_curr, 
        curr_from.currency_code as to_curr, 
        curr_opt.currency_from_idx as from_curr_idx, 
        curr_opt.currency_to_idx as to_curr_idx, 
        curr_rate.valid_from_date as eff_start_date, 
        curr_rate.valid_to_date as eff_end_date, 
        curr_rate.value as conversion_rate,
        1/curr_rate.value as inversion_rate
    from curr_opt
    inner join curr_rate
    on curr_opt.option_idx = curr_rate.option_idx
    inner join dim_currency curr_to
    on curr_opt.currency_from_idx = curr_to.currency_idx
    inner join dim_currency curr_from
    on curr_opt.currency_to_idx = curr_from.currency_idx
)
select 
    from_curr,
	to_curr,
	from_curr_idx,
	to_curr_idx,
	eff_start_date,
	eff_end_date,
	conversion_rate,
	inversion_rate
from final

