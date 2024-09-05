{{
    config(
        tags = ["sales","ibe_forecast","ibe"]
    )
}}

with
source as (select * from {{ ref("src_wbx_ibe_forecast") }}),

renamed as (

    select
        trim(trade_type) as tratypcde,
        case when length(item_number) < 5 then to_char(
            lpad(trim(item_number), 5, 0)
        ) else trim(item_number) end as comcde5d,
        ---Shift the Periods to Fiscal Period from Calendar period.  Oct is period 1 through Sep as period 12
        case
            when calendar_month_no between 1 and 3 then calendar_year - 1 else
                calendar_year
        end as cyr,
        case
            when calendar_month_no between 1 and 3 then calendar_month_no + 9
            when calendar_month_no between 4 and 12 then calendar_month_no - 3
            else calendar_month_no
        end as cyrper,
        forecast_ca_qty as budqty,
        0 as wasteredval,
        0 as cleaningval,
        0 as valueeng,
        0 as labouradjval,
        0 as budkgs,
        0 as budpallets,
        0 as avpkgs,
        0 as kgsbars,
        gross_amount as grossvaluetotal,
        edlptotal as edlptotal,
        rsatotal as rsatotal,
        settlement_total as settlementtotal,
        gincent_total as gincenttotal,
        incentive_forced as incentiveforced,
        add_incent_total as addincenttotal,
        other_total as othertotal,
        back_margin_total as backmargintotal,
        gross_amount
        + edlptotal
        + rsatotal
        + settlement_total
        + gincent_total
        + incentive_forced
        + add_incent_total
        + other_total
        + back_margin_total
            as netvaluetotal,
        avp_gross_up as avpgrossup,
        gross_amount
        + edlptotal
        + rsatotal
        + settlement_total
        + gincent_total
        + incentive_forced
        + add_incent_total
        + other_total
        + back_margin_total
        + avp_gross_up as netvalgrossup,
        raw_materials_total as rawmaterialstotal,
        packaging_total as packagingtotal,
        labour_total as labourtotal,
        bought_in_total as boughtintotal,
        co_packing_total as copackingtotal,
        rye_adj_total as ryeadjtotal,
        total_cost as totalcost,
        0 as exptradespend,
        0 as expconsumerspend,
        0 as "PIF-ISA",
        0 as "PIF-TRADE",
        0 as "PIF-TRADE OIB",
        0 as "PIF-TRADE RED",
        0 as "PIF-TRADE AVP",--
        pif_trade_enh as "PIF-TRADE ENH",
        0 as "MIF-CATEGORY",
        mif_customer_marketing as "MIF-CUSTOMER MARKETING",--
        0 as "MIF-FIELD MARKETING",
        0 as "MIF-ISA",
        0 as "MIF-RANGE SUPPORT INCENTIVE",
        0 as "MIF-TRADE",
        0 as isaextra,
        fixed_annual_payment as fixedannualpayment,
        consumer_marketing as consumermarketing,
        rsatotal_new,
        '' as product_group,
        'DAILY' as frozen_forecast,
        site as company_code
    from source

),

final_cast as (
    select
        cast(tratypcde as VARCHAR(256)) as tratypcde,
        cast(comcde5d as VARCHAR(8)) as comcde5d,
        cast(cyr as NUMBER(38, 0)) as cyr,
        cast(cyrper as NUMBER(38, 0)) as cyrper,
        cast(budqty as FLOAT) as budqty,
        cast(wasteredval as FLOAT) as wasteredval,
        cast(cleaningval as FLOAT) as cleaningval,
        cast(valueeng as FLOAT) as valueeng,
        cast(labouradjval as FLOAT) as labouradjval,
        cast(budkgs as FLOAT) as budkgs,
        cast(budpallets as FLOAT) as budpallets,
        cast(avpkgs as FLOAT) as avpkgs,
        cast(kgsbars as FLOAT) as kgsbars,
        cast(grossvaluetotal as FLOAT) as grossvaluetotal,
        cast(edlptotal as FLOAT) as edlptotal,
        cast(rsatotal as FLOAT) as rsatotal,
        cast(settlementtotal as FLOAT) as settlementtotal,
        cast(gincenttotal as FLOAT) as gincenttotal,
        cast(incentiveforced as FLOAT) as incentiveforced,
        cast(addincenttotal as FLOAT) as addincenttotal,
        cast(othertotal as FLOAT) as othertotal,
        cast(backmargintotal as FLOAT) as backmargintotal,
        cast(netvaluetotal as FLOAT) as netvaluetotal,
        cast(avpgrossup as FLOAT) as avpgrossup,
        cast(netvalgrossup as FLOAT) as netvalgrossup,
        cast(rawmaterialstotal as FLOAT) as rawmaterialstotal,
        cast(packagingtotal as FLOAT) as packagingtotal,
        cast(labourtotal as FLOAT) as labourtotal,
        cast(boughtintotal as FLOAT) as boughtintotal,
        cast(copackingtotal as FLOAT) as copackingtotal,
        cast(ryeadjtotal as FLOAT) as ryeadjtotal,
        cast(totalcost as FLOAT) as totalcost,
        cast(exptradespend as FLOAT) as exptradespend,
        cast(expconsumerspend as FLOAT) as expconsumerspend,
        cast("PIF-ISA" as FLOAT) as "PIF-ISA",
        cast("PIF-TRADE" as FLOAT) as "PIF-TRADE",
        cast("PIF-TRADE OIB" as FLOAT) as "PIF-TRADE OIB",
        cast("PIF-TRADE RED" as FLOAT) as "PIF-TRADE RED",
        cast("PIF-TRADE AVP" as FLOAT) as "PIF-TRADE AVP",
        cast("PIF-TRADE ENH" as FLOAT) as "PIF-TRADE ENH",
        cast("MIF-CATEGORY" as FLOAT) as "MIF-CATEGORY",
        cast("MIF-CUSTOMER MARKETING" as FLOAT) as "MIF-CUSTOMER MARKETING",
        cast("MIF-FIELD MARKETING" as FLOAT) as "MIF-FIELD MARKETING",
        cast("MIF-ISA" as FLOAT) as "MIF-ISA",
        cast("MIF-RANGE SUPPORT INCENTIVE" as FLOAT)
            as "MIF-RANGE SUPPORT INCENTIVE",
        cast("MIF-TRADE" as FLOAT) as "MIF-TRADE",
        cast(isaextra as NUMBER(38, 0)) as isaextra,
        cast(fixedannualpayment as FLOAT) as fixedannualpayment,
        cast(consumermarketing as FLOAT) as consumermarketing,
        cast(rsatotal_new as FLOAT) as rsatotal_new,  
        cast(substring(product_group, 1, 255) as TEXT(255)) as product_group,
        cast(substring(frozen_forecast, 1, 100) as TEXT(100))
            as frozen_forecast,
        cast(substring(company_code, 1, 10) as TEXT(10)) as company_code
    from renamed
)

select * from final_cast
