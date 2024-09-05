
   with  d365_source as (
        select *
        from {{ source("D365", "invent_table") }} where _FIVETRAN_DELETED='FALSE' and upper(trim(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} 

    ),

    renamed as (
        select
            'D365' as source,
            item_id as itemid,
            item_type as itemtype,
            purch_model as purchmodel,
            height as height,
            width as width,
            sales_model as salesmodel,
            cost_group_id as costgroupid,
            req_group_id as reqgroupid,
            null as epcmanager,
            primary_vendor_id as primaryvendorid,
            net_weight as netweight,
            depth as depth,
            unit_volume as unitvolume,
            bomunit_id as bomunitid,
            null as itempricetolerancegroupid,
            density as density,
            cost_model as costmodel,
            use_alt_item_id as usealtitemid,
            alt_item_id as altitemid,
            matching_policy as matchingpolicy,
            null as intracode,
            prod_flushing_princip as prodflushingprincip,
            minimumpalletquantity as minimumpalletquantity,
            null as pbaitemautogenerated,
            wmsarrival_handling_time as wmsarrivalhandlingtime,
            bommanual_receipt as bommanualreceipt,
            phantom as phantom,
            null as intraunit,
            bomlevel as bomlevel,
            batch_num_group_id as batchnumgroupid,
            auto_report_finished as autoreportfinished,
            orig_country_region_id as origcountryregionid,
            statistics_factor as statisticsfactor,
            null as altconfigid,
            null as standardconfigid,
            null as prodpoolid,
            null as propertyid,
            abctie_up as abctieup,
            abcrevenue as abcrevenue,
            abcvalue as abcvalue,
            abccontribution_margin as abccontributionmargin,
            null as commissiongroupid,
            sales_percent_markup as salespercentmarkup,
            sales_contribution_ratio as salescontributionratio,
            sales_price_model_basic as salespricemodelbasic,
            name_alias as namealias,
            null as prodgroupid,
            proj_category_id as projcategoryid,
            gross_depth as grossdepth,
            gross_width as grosswidth,
            gross_height as grossheight,
            standardpalletquantity as standardpalletquantity,
            qtyperlayer as qtyperlayer,
            sort_code as sortcode,
            serial_num_group_id as serialnumgroupid,
            item_buyer_group_id as itembuyergroupid,
            tax_packaging_qty as taxpackagingqty,
            null as wmspallettypeid,
            null as origstateid,
            wmspickingqtytime as wmspickingqtytime,
            tara_weight as taraweight,
            null as packaginggroupid,
            scrap_var as scrapvar,
            scrap_const as scrapconst,
            null as standardinventcolorid,
            standard_invent_size_id as standardinventsizeid,
            item_dim_cost_price as itemdimcostprice,
            null as altinventsizeid,
            null as altinventcolorid,
            forecast_dmpinclude as forecastdmpinclude,
            product as product,
            null as pallettagging,
            null as itemtagginglevel,
            default_dimension as defaultdimension,
            fiscal_lifoavoid_calc as fiscallifoavoidcalc,
            fiscal_lifonormal_value as fiscallifonormalvalue,
            fiscal_lifonormal_value_calc as fiscallifonormalvaluecalc,
            bomcalc_group_id as bomcalcgroupid,
            invent_fiscal_lifogroup as inventfiscallifogroup,
            ngpcodes_table_fr as ngpcodestable_fr,
            null as origcountyid,
            null as taxfiscalclassification_br,
            null as pbaitemconfigurable,
            null as pbainventitemgroupid,
            null as pbahidedialog,
            null as pbahideapproval,
            null as pbaautostart,
            null as pbamandatoryconfig,
            null as inventproducttype_br,
            taxation_origin_br as taxationorigin_br,
            null as taxservicecode_br,
            excise_tariff_codes_in as excisetariffcodes_in,
            customs_export_tariff_code_table_in as customsexporttariffcodetbl_in,
            customs_import_tariff_code_table_in as customsimporttariffcodetbl_in,
            service_code_table_in as servicecodetable_in,
            exim_product_group_table_in as eximproductgrouptable_in,
            null as packing_ru,
            null as assetgroupid_ru,
            null as assetid_ru,
            intrastat_exclude as intrastatexclude,
            null as intrastatprocid_cz,
            null as pkwiucode_pl,
            null as exceptioncode_br,
            icmson_service_br as icmsonservice_br,
            pdscwwmsstandardpalletqty as pdscwwmsstandardpalletqty,
            pdscwwmsminimumpalletqty as pdscwwmsminimumpalletqty,
            pdscwwmsqtyperlayer as pdscwwmsqtyperlayer,
            null as alcoholmanufacturerid_ru,
            null as alcoholproductiontypeid_ru,
            alcohol_strength_ru as alcoholstrength_ru,
            null as altinventstyleid,
            approx_tax_value_br as approxtaxvalue_br,
            batch_merge_date_calculation_method as batchmergedatecalcmethod,
            null as daxintegrationkey,
            null as markupcode_ru,
            null as nrtaxgroup_lv,
            null as pdsbaseattributeid,
            pds_best_before as pdsbestbefore,
            null as pdsfreightallocationgroupid,
            null as pdsitemrebategroupid,
            pdspotency_attrib_recording as pdspotencyattribrecording,
            pds_shelf_advice as pdsshelfadvice,
            pds_shelf_life as pdsshelflife,
            pdstarget_factor as pdstargetfactor,
            pds_vendor_check_item as pdsvendorcheckitem,
            null as pmfplanningitemid,
            pmf_product_type as pmfproducttype,
            pmf_yield_pct as pmfyieldpct,
            null as sadratecode_pl,
            skip_intra_company_sync_ru as skipintracompanysync_ru,
            null as standardinventstyleid,
            dsa_in as dsa_in,
            excise_record_type_in as exciserecordtype_in,
            modifieddatetime as modifieddatetime,
            null as del_modifiedtime,
            modifiedby as modifiedby,
            createddatetime as createddatetime,
            null as del_createdtime,
            createdby as createdby,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as wbxautomaticformulachange,
            null as wbxcycletimessc,
            service_accounting_code_table_in as serviceaccountingcodetable_in,
            exempt_in as exempt_in,
            hsncode_table_in as hsncodetable_in,
            null as includedemandforecast,
            null as avpweight,
            null as consumerunit,
            null as currentflag,
            null as inventbrandingcodeid,
            null as inventpacksizecodeid,
            null as inventproductclasscodeid,
            null as inventstrategiccodeid,
            null as inventsubproductcodeid,
            null as pmpflag,
            null as avpflag,
            null as satcodeid_mx,
            null as undershelflife,
            null as overshelflife

        from d365_source

    )

select *
from renamed 
