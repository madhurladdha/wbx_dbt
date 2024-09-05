{{ config(
    tags=["wbx", "manufacturing", "stock", "yield"],
    snowflake_warehouse=env_var("DBT_WBX_SF_WH")
) }}

--variables to enable incremental and full load toggle.
{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with
    dim_attributevaluesetitem_view as (
        select * from {{ ref("src_dim_attributevaluesetitem_view") }}
    ),

    dimensionattribute as (select * from {{ ref("src_dimensionattribute") }}),

    inventtransorigin as (select * from {{ ref("src_inventtransorigin") }}),

    inventtrans as (select * from {{ ref("src_inventtrans") }}),

    inventtransposting as (select * from {{ ref("src_inventtransposting") }}),

    inventmodelgroupitem as (select * from {{ ref("src_inventmodelgroupitem") }}),

    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),

    inventdim as (select * from {{ ref("src_inventdim") }}),

    dm1 as (
        select dalvv.displayvalue, dimensionattributevalueset, da.name, da.partition
        from dim_attributevaluesetitem_view dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'Sites'
    ),

    dm2 as (
        select dalvv.displayvalue, dimensionattributevalueset, da.name, da.partition
        from dim_attributevaluesetitem_view dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'CostCenters'
    ),
    dm4 as (
        select dalvv.displayvalue, dimensionattributevalueset, da.name, da.partition
        from dim_attributevaluesetitem_view dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'Plant'
    ),

    dm6 as (
        select dalvv.displayvalue, dimensionattributevalueset, da.name, da.partition
        from dim_attributevaluesetitem_view dalvv
        inner join dimensionattribute da on dalvv.dimensionattribute = da.recid
        where da.name = 'ProductClass'
    ),

    stage_table as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            ito.inventtransid as source_transaction_key,
            it.recid as source_record_id,
            ito.referenceid as related_document_number,
            it.itemid as source_item_identifier,
            upper(nvl(dm4.displayvalue, '')) as source_business_unit_code,
            id.inventsizeid as variant_code,
            it.datefinancial as transaction_date,
            itp.transbegintime as gl_date,
            it.qty as transaction_qty,
            it.costamountposted as transaction_amount,
            itm.unitid as transaction_uom,
            it.currencycode as transaction_currency,
            upper(
                case
                    it.statusreceipt when 0 then it.statusissue else it.statusreceipt
                end
            ) as status_code,
            case
                it.statusreceipt
                when 0
                then
                    case
                        it.statusissue
                        when 0
                        then 'None'
                        when 1
                        then 'Sold'
                        when 2
                        then 'Deducted'
                        when 3
                        then 'Picked'
                        when 4
                        then 'Reserved Physical'
                        when 5
                        then 'Reserved Ordered'
                        when 6
                        then 'On Order'
                        when 7
                        then 'Quotation Issue'
                    end
                else
                    case
                        it.statusreceipt
                        when 0
                        then 'None'
                        when 1
                        then 'Purchased'
                        when 2
                        then 'Received'
                        when 3
                        then 'Registered'
                        when 4
                        then 'Arrived'
                        when 5
                        then 'Ordered'
                        when 6
                        then 'Quotation Receipt'
                    end
            end as status_desc,
            it.voucher,
            it.costamountadjustment as adjustment_amt,
            current_timestamp() as load_date,
            current_timestamp() as update_date,
            upper(it.dataareaid) as company_code,
            nvl(dm1.displayvalue, '') as site,
            nvl(dm6.displayvalue, '') as product_class,
            id.inventsiteid as stock_site,
            it.invoicereturned as invoice_returned_flag,
            upper(imgi.modelgroupid) as item_model_group
        from inventtransorigin ito
        inner join
            inventtrans it
            on ito.recid = it.inventtransorigin
            and ito.partition = it.partition
            and ito.dataareaid = it.dataareaid
        inner join
            inventdim id
            on it.inventdimid = id.inventdimid
            and it.dataareaid = id.dataareaid
            and it.partition = id.partition
        inner join
            inventtablemodule itm
            on it.partition = itm.partition
            and it.dataareaid = itm.dataareaid
            and it.itemid = itm.itemid
            and 0 = itm.moduletype
        inner join
            inventmodelgroupitem imgi
            on it.partition = imgi.partition
            and it.dataareaid = imgi.itemdataareaid
            and it.itemid = imgi.itemid
        inner join
            (
                select distinct
                    inventtransorigin,
                    voucher,
                    dataareaid,
                    partition,
                    defaultdimension,
                    transbegintime
                from inventtransposting
                where defaultdimension <> 0
            ) itp
            on itp.inventtransorigin = ito.recid
            and itp.partition = ito.partition
            and itp.voucher = it.voucher  -- AND ISPOSTED=1
        left outer join
            dm1
            on itp.defaultdimension = dm1.dimensionattributevalueset
            and itp.partition = dm1.partition
        left outer join
            dm4
            on itp.defaultdimension = dm4.dimensionattributevalueset
            and itp.partition = dm4.partition
        left outer join
            dm6
            on itp.defaultdimension = dm6.dimensionattributevalueset
            and itp.partition = dm6.partition
            --logic for incremental pull on weekdays and full data pull on weekends
        {% if day_today != full_load_day %}
            {% if not flags.FULL_REFRESH %}
                where
                    it.datefinancial >= dateadd(
                        'DAY',
                       -{{ env_var("DBT_STD_INCR_LOOKBACK") }},
                        to_date(convert_timezone('UTC', current_timestamp))
                    )
            {% endif %}
        {% endif %}
    ),

    final as (
        select
            source_system,
            source_transaction_key,
            source_record_id,
            related_document_number,
            source_item_identifier,
            source_business_unit_code,
            variant_code,
            transaction_date,
            gl_date,
            transaction_qty,
            transaction_amount as transaction_amt,
            transaction_uom,
            transaction_currency,
            status_code,
            status_desc,
            voucher,
            adjustment_amt,
            load_date,
            update_date,
            company_code,
            site,
            product_class,
            stock_site,
            invoice_returned_flag,
            item_model_group
        from stage_table
    )

select *
from final
