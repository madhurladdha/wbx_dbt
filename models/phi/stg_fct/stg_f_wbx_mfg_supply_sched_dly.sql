{{ config(tags=["manufacturing", "supply_Schedule", "wbx", "daily","inventory"]) }}

with
    reqtrans as (select * from {{ ref("src_reqtrans") }}),
    reqplanversion as (select * from {{ ref("src_reqplanversion") }}),
    reqpo as (select * from {{ ref("src_reqpo") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    source_qualifier as (
        select
            '{{ env_var("DBT_SOURCE_SYSTEM") }}' as source_system,
            rt.refid as document_number,
            to_date(convert_timezone('UTC', current_timestamp)) as snapshot_date,
            rt.reftype as transaction_type_code,
            case
                when
                    upper(trim(id.inventlocationid)) = null
                    or upper(trim(id.inventlocationid)) = ''
                then upper(id.inventsiteid)
                else upper(trim(id.inventlocationid))
            end as source_business_unit_code,
            rt.itemid as source_item_identifier,
            id.inventsizeid as variant_code,
            upper(rt.dataareaid) as source_company_code,
            id.inventsiteid as source_site_code,
            case
                when
                    rt.reftype = 1
                    and to_date(rt.reqdate) = to_date('01-JAN-1900', 'DD-MON-YYYY')
                then to_date(convert_timezone('UTC', current_timestamp)) - 1
                when
                    rt.reftype = 47
                    and to_date(rt.reqdate) = to_date('10-NOV-1900', 'DD-MON-YYYY')
                then to_date(convert_timezone('UTC', current_timestamp)) - 1
                else rt.reqdate
            end as transaction_date,
            cast(
                case
                    rt.reftype
                    when 0
                    then 'None'
                    when 1
                    then 'InventOnHand'
                    when 8
                    then 'Purchase order'
                    when 9
                    then 'Production'
                    when 10
                    then 'Sales order'
                    when 12
                    then 'Production line'
                    when 13
                    then 'Stock journal'
                    when 14
                    then 'Safety stock'
                    when 15
                    then 'Stock journal transfer'
                    when 16
                    then 'Transfer order shipment'
                    when 17
                    then 'Transfer order receive'
                    when 21
                    then 'Demand forecast'
                    when 31
                    then 'Planned works orders'
                    when 32
                    then 'BOM line'
                    when 33
                    then 'Planned purchase orders'
                    when 34
                    then 'Planned transfer'
                    when 35
                    then 'Transfer requirement'
                    when 45
                    then 'Formula line'
                    when 46
                    then 'Planned batch order'
                    when 47
                    then 'Expired batch'
                    else '***UNKNOWN***'
                end as varchar(255)
            ) as transaction_desc,
            rpv.reqplanid as plan_version,
            rt.direction as transaction_direction_code,
            cast(
                case
                    direction
                    when 0
                    then 'None'
                    when 1
                    then 'Receipt'
                    when 2
                    then 'Issue'
                end as varchar(255)
            ) as transaction_direction_desc,
            sum(rt.qty) as transaction_quantity,
            sum(rt.originalquantity) as original_quantity,
            case
                nvl(rp.reqpostatus, -1)
                when 0
                then 'UNPLANNED'
                when 1
                then 'PLANNED'
                when 2
                then 'PLANNED'
                else ''
            end as transaction_planned_flag
        from reqtrans rt
        inner join
            inventdim id
            on rt.covinventdimid = id.inventdimid
            and upper(rt.dataareaid) = upper(id.dataareaid)
            and rt.partition = id.partition
        inner join
            reqplanversion rpv
            on rt.planversion = rpv.recid
            and rt.partition = rpv.partition
            and upper(rt.dataareaid) = upper(rpv.reqplandataareaid)
        left outer join
            reqpo rp
            on rt.refid = rp.refid
            and rp.reftype = 46
            and rp.planversion = rt.planversion
            and rt.reftype = 45
        where upper(trim(rt.dataareaid)) in {{env_var("DBT_COMPANY_FILTER")}} and rpv.active = 1
        --27th December,2023 : (rt.dataareaid = 'wbx')  --Added filter to include all the dataarea id here 
        group by
            rt.refid,
            id.inventlocationid,
            rt.itemid,
            id.inventsizeid,
            rt.dataareaid,
            id.inventsiteid,
            rt.reqdate,
            rt.reftype,
            rpv.reqplanid,
            rt.direction,
            rp.reqpostatus
    )

select * from source_qualifier
