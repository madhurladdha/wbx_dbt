with
    stg as (select * from {{ ref("src_wbx_item_inflation") }}),

    final as (
        select
            cast(LTRIM(RTRIM(split_part(item_buyergroup, '-', 1))) as varchar2(60)) as buyer_code,
            cast(
                LTRIM(RTRIM(split_part(item_buyergroup, '-', 2))) as varchar2(60)
            ) as buyer_code_description,
            cast(year as varchar2(60)) as year,
            cast(substring((upper(scenario)), 1, 60) as varchar2(60)) as scenario,
            cast(substring((oct), 1, 60) as varchar2(60)) as oct,
            cast(substring((nov), 1, 60) as varchar2(60)) as nov,
            cast(substring((dec), 1, 60) as varchar2(60)) as dec,
            cast(substring((jan), 1, 60) as varchar2(60)) as jan,
            cast(substring((feb), 1, 60) as varchar2(60)) as feb,
            cast(substring((mar), 1, 60) as varchar2(60)) as mar,
            cast(substring((apr), 1, 60) as varchar2(60)) as apr,
            cast(substring((may), 1, 60) as varchar2(60)) as may,
            cast(substring((jun), 1, 60) as varchar2(60)) as jun,
            cast(substring((jul), 1, 60) as varchar2(60)) as jul,
            cast(substring((aug), 1, 60) as varchar2(60)) as aug,
            cast(substring((sep), 1, 60) as varchar2(60)) as sep,
            current_timestamp as load_date
        from stg
    )

select *
from final
