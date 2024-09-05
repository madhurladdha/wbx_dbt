with
    x as (select * from {{ ref("src_exc_fact_promotion_customer_sublevel") }}),
    y as (select * from {{ ref("src_exc_dim_promotions") }}),
    a as (select * from {{ ref("src_exc_dim_pc_customer") }}),
    b as (select * from {{ ref("src_exc_fact_pc_customer") }}),
    c as (select * from {{ ref("src_exc_dim_customer_levels") }}),
    final as (
        select
            y.promo_code,
            x.promo_idx,
            a.code as cust_code,
            a.idx as cust_idx,
            c.custlevel_code,
            c.custlevel_name,
            count(0) over (partition by x.promo_idx, c.custlevel_code) as cust_count
        from x
        inner join y on x.promo_idx = y.promo_idx
        inner join a on x.sub_cust_idx = a.idx
        inner join b on a.idx = b.idx
        inner join c on b.custlevel_idx = c.custlevel_idx
        where hierarchy_idx = 1
    )

select *
from final
