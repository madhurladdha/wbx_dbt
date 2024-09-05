{{
    config(
        tags=["manufacturing", "manufacturing_plan", "manufacturing_plan_Calendar"],
    )
}}


with
    cte_workperiodtemplate as (
        select
            wpt.name,
            wpt.fixeddaystart,
            wpt.worktimeid,
            wpt.worktimeiddataareaid,
            wpt.legalentitydefault,
            wptl.linenumber,
            wptl.period,
            wptl.numberofperiods,
            wptl.explodeperiods,
            wptl.perioddescription,
            next_day(convert_timezone('UTC', current_timestamp), 'SUNDAY')
            - 7 as fromdate,
            next_day(convert_timezone('UTC', current_timestamp), 'SUNDAY') - 1 as todate
        from {{ ref("src_workperiodtemplate") }} wpt
        inner join
            {{ ref("src_workperiodtemplateline") }} wptl
            on wpt.recid = wptl.periodtemplate
        where explodeperiods = 1 and period = 1
    ),
    dt(
        name,
        fixeddaystart,
        worktimeid,
        worktimeiddataareaid,
        legalentitydefault,
        linenumber,
        period,
        numberofperiods,
        explodeperiods,
        perioddescription,
        fromdate,
        todate
    ) as (select * from cte_workperiodtemplate),
    ctecounter(
        name,
        n,
        numberofperiods,
        period,
        fromdate,
        todate,
        perioddescription
    ) as (
        select
            name, 0 as n, numberofperiods, period, fromdate, todate, perioddescription
        from dt
        union all
        select
            name,
            n + 1,
            numberofperiods,
            period,
            case period when 1 then fromdate + 7 end as fromdate,
            case period when 1 then todate + 7 end as todate,
            perioddescription
        from ctecounter
        where n < ctecounter.numberofperiods
    ),
    cte_final_src as (
        select
            name,
            'Backlog' as description,
            to_date('1900/01/01', 'YYYY/MM/DD') as fromdate,
            dateadd(
                day, -1, to_date(convert_timezone('UTC', current_timestamp))
            ) as todate,
            0 as open_days
        from ctecounter
        where n = 0
        union
        select
            name,
            case
                n
                when numberofperiods
                then 'Outlook'
                else
                    replace(
                        replace(
                            cast(perioddescription as varchar2(255)),
                            '%4',
                            cast(
                                to_char(
                                    weekofyear(convert_timezone('UTC', todate))
                                ) as varchar2(255)
                            )
                        ),
                        '%5',
                        cast(
                            to_char(
                                weekofyear(convert_timezone('UTC', todate))
                            ) as varchar2(255)
                        )
                    )
            end as description,
            case
                n
                when 0
                then to_date(convert_timezone('UTC', current_timestamp))
                else fromdate
            end as fromdate,
            case
                n
                when numberofperiods
                then to_date('2154/12/31', 'YYYY/MM/DD')
                else todate
            end as todate,
            case
                n
                when numberofperiods
                then 0
                else
                    datediff(
                        day,
                        case
                            n
                            when 0
                            then to_date(convert_timezone('UTC', current_timestamp))
                            else fromdate
                        end,
                        todate
                    )
                    + 1
            end as open_days
        from ctecounter
        order by name, fromdate
    )
select
    upper(name) as planning_calendar_name,
    upper(description) as week_description,
    fromdate as week_start_date,
    todate as week_end_date,
    open_days,
    '{{ env_var("DBT_SOURCE_SYSTEM"'')}}' as source_system,
    systimestamp() as load_date,
    systimestamp() as update_date
from cte_final_src
