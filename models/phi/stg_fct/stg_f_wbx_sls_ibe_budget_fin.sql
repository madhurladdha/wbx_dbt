{{
    config(
        tags = ["sales","ibe_budget","ibe_sls_budget","ibe_sls_budget_fin","adhoc"]
    )
}}

with 
source as (select * from {{ref("src_wbx_ibe_budget_sku_pl")}}),

renamed as (

    select
        trim(tratypcde) as tratypcde,
        case when LENGTH(comcde_5_d) < 5 then to_char(
                        lpad(trim(comcde_5_d), 5, 0)
                    ) else trim(comcde_5_d) end  as comcde5d,
        ---Shift the Periods to Fiscal Period from Calendar period.  Oct is period 1 through Sep as period 12
        case when cyrper between 1 and 3 then cyr-1 else cyr end as cyr,
        case 
            when cyrper between 1 and 3 then cyrper+9
            when cyrper between 4 and 12 then cyrper-3
            else cyrper end as cyrper,
        budqty,
        waste_red_val as wasteredval,
        cleaning_val as cleaningval,
        value_eng as valueeng,
        labour_adj_val as labouradjval,
        budkgs,
        budpallets,
        avpkgs,
        kgs_bars as kgsbars,
        gross_value_total as grossvaluetotal,
        edlptotal,
        rsatotal,
        settlement_total as settlementtotal,
        gincent_total as gincenttotal,
        incentive_forced as incentiveforced,
        add_incent_total as addincenttotal,
        other_total as othertotal,
        back_margin_total as backmargintotal,
        net_value_total as netvaluetotal,
        avp_gross_up as avpgrossup,
        net_val_gross_up as netvalgrossup,
        raw_materials_total as rawmaterialstotal,
        packaging_total as packagingtotal,
        labour_total as labourtotal,
        bought_in_total as boughtintotal,
        co_packing_total as copackingtotal,
        rye_adj_total as ryeadjtotal,
        total_cost as totalcost,
        exp_trade_spend as exptradespend,
        exp_consumer_spend as expconsumerspend,
        pif_isa as "PIF-ISA",
        pif_trade as "PIF-TRADE",
        pif_trade_oib as "PIF-TRADE OIB",
        pif_trade_red as "PIF-TRADE RED",
        pif_trade_avp as "PIF-TRADE AVP",
        pif_trade_enh as "PIF-TRADE ENH",
        mif_category as "MIF-CATEGORY",
        mif_customer_marketing as "MIF-CUSTOMER MARKETING",
        mif_field_marketing as "MIF-FIELD MARKETING",
        mif_isa as "MIF-ISA",
        mif_range_support_incentive as "MIF-RANGE SUPPORT INCENTIVE",
        mif_trade as "MIF-TRADE",
        isa_extra as isaextra,
        product_group,
        frozen_forecast as frozen_forecast
    from source

),

final_cast as (
    select 
        cast(tratypcde as VARCHAR(256)) as tratypcde,
        cast(comcde5d as VARCHAR(8) ) as comcde5d  ,
        cast(cyr as number(38,0) ) as cyr  ,
        cast(cyrper as number(38,0) ) as cyrper  ,
        cast(budqty as float) as budqty  ,
        cast(wasteredval as float) as wasteredval  ,
        cast(cleaningval as float) as cleaningval  ,
        cast(valueeng as float) as valueeng  ,
        cast(labouradjval as float) as labouradjval  ,
        cast(budkgs as float) as budkgs  ,
        cast(budpallets as float) as budpallets  ,
        cast(avpkgs as float) as avpkgs  ,
        cast(kgsbars as float) as kgsbars  ,
        cast(grossvaluetotal as float) as grossvaluetotal  ,
        cast(edlptotal as float) as edlptotal  ,
        cast(rsatotal as float) as rsatotal  ,
        cast(settlementtotal as float) as settlementtotal  ,
        cast(gincenttotal as float) as gincenttotal  ,
        cast(incentiveforced as float) as incentiveforced  ,
        cast(addincenttotal as float) as addincenttotal  ,
        cast(othertotal as float) as othertotal  ,
        cast(backmargintotal as float) as backmargintotal  ,
        cast(netvaluetotal as float) as netvaluetotal  ,
        cast(avpgrossup as float) as avpgrossup  ,
        cast(netvalgrossup as float) as netvalgrossup  ,
        cast(rawmaterialstotal as float) as rawmaterialstotal  ,
        cast(packagingtotal as float) as packagingtotal  ,
        cast(labourtotal as float) as labourtotal  ,
        cast(boughtintotal as float) as boughtintotal  ,
        cast(copackingtotal as float) as copackingtotal  ,
        cast(ryeadjtotal as float) as ryeadjtotal  ,
        cast(totalcost as float) as totalcost  ,
        cast(exptradespend as float) as exptradespend  ,
        cast(expconsumerspend as float) as expconsumerspend  ,
        cast("PIF-ISA" as float) as "PIF-ISA"  ,
        cast("PIF-TRADE" as float) as "PIF-TRADE"  ,
        cast("PIF-TRADE OIB" as float) as "PIF-TRADE OIB"  ,
        cast("PIF-TRADE RED" as float) as "PIF-TRADE RED"  ,
        cast("PIF-TRADE AVP" as float) as "PIF-TRADE AVP"  ,
        cast("PIF-TRADE ENH" as float) as "PIF-TRADE ENH"  ,
        cast("MIF-CATEGORY" as float) as "MIF-CATEGORY"  ,
        cast("MIF-CUSTOMER MARKETING" as float) as "MIF-CUSTOMER MARKETING"  ,
        cast("MIF-FIELD MARKETING" as float) as "MIF-FIELD MARKETING"  ,
        cast("MIF-ISA" as float) as "MIF-ISA"  ,
        cast("MIF-RANGE SUPPORT INCENTIVE" as float) as "MIF-RANGE SUPPORT INCENTIVE"  ,
        cast("MIF-TRADE" as float) as "MIF-TRADE"  ,
        cast(isaextra as number(38,0) ) as isaextra  ,
        cast(substring(product_group,1,255) as text(255) ) as product_group  ,
        cast(substring(frozen_forecast,1,100) as text(100) ) as frozen_forecast
    from renamed
)

select * from final_cast
