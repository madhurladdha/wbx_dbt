{{
    config(

    tags = ["ppv","procurement","forecast_exchange_rate"]
          )
}}






with src as(
    select * from {{ref('src_wbx_forecast_exchange_rate')}}
),

Final as(
select year,
        upper(scenario) as scenario,
        oct,
        nov,
        dec,
        jan,
        feb,
        mar,
        apr,
        may,
        jun,
        jul,
        aug,
        sep,
        SYSTIMESTAMP() as load_date
        from src
)


select  
cast(substring(year,1,60) as text(60) ) as year  ,
cast(substring(scenario,1,60) as text(60) ) as scenario  ,
cast(oct as number(38,10) ) as oct  ,
cast(nov as number(38,10) ) as nov  ,
cast(dec as number(38,10) ) as dec  ,
cast(jan as number(38,10) ) as jan  ,
cast(feb as number(38,10) ) as feb  ,
cast(mar as number(38,10) ) as mar  ,
cast(apr as number(38,10) ) as apr  ,
cast(may as number(38,10) ) as may  ,
cast(jun as number(38,10) ) as jun  ,
cast(jul as number(38,10) ) as jul  ,
cast(aug as number(38,10) ) as aug  ,
cast(sep as number(38,10) ) as sep  ,
cast(load_date as timestamp_ntz(9) ) as load_date

from final