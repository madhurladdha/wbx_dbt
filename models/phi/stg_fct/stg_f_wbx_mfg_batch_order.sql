{{
    config(
        tags=[
            "manufacturing",
            "batch",
            "wbx",
        ],
    )
}}
with
    reqpo as (select * from {{ ref("src_reqpo") }}),
    reqtrans as (select * from {{ ref("src_reqtrans") }}),
    reqplanversion as (select * from {{ ref("src_reqplanversion") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),

    source as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            to_date(convert_timezone('UTC', current_timestamp())) as snapshot_date,
            rp.refid as source_transaction_key,
            id.inventlocationid as source_business_unit_code,
            (rp.dataareaid) as source_company,
            rp.itemid as source_item_identifier,
            id.inventsizeid as variant_code,
            rp.reqdatedlv as transaction_eff_date,
            rp.reftype as transaction_type_code,

            case
                rp.reftype
                when 31
                then 'Planned works orders'
                when 33
                then 'Planned purchase orders'
                when 34
                then 'Planned transfer'
                when 46
                then 'Planned batch order'
                else 'Unknown REFTYPE'
            end as transaction_desc,

            rp.reqpostatus as transaction_status_code,

            case
                rp.reqpostatus
                when 0
                then 'Unprocessed'
                when 1
                then 'Completed'
                when 2
                then 'Approved'
            end as transaction_status_desc,

            rpv.reqplanid as plan_version,
            rp.qty as transaction_quantity,
            rp.costamount as transaction_amount,
            rp.itembomid as reference_text
        from reqpo rp
        inner join
            reqtrans rt
            on rp.planversion = rt.planversion
            and rp.reftype = rt.reftype
            and rp.refid = rt.refid
        inner join reqplanversion rpv on rp.planversion = rpv.recid
        inner join
            inventdim id
            on upper(rp.dataareaid) = upper(id.dataareaid)
            and rp.covinventdimid = id.inventdimid
    ),

    tfm as (
    select 
            source_system,
            snapshot_date,
            source_transaction_key,
            source_business_unit_code,
            upper(source_company) as source_company,
            source_item_identifier,
            variant_code,
            transaction_eff_date,
            to_char(to_number(transaction_type_code))	as transaction_type_code,
            transaction_desc,
            to_char(to_number(transaction_status_code))	as transaction_status_code,
            transaction_status_desc,
            plan_version,
            transaction_quantity,
            transaction_amount,
            reference_text,
            current_timestamp() as source_updated_date,
            current_timestamp() as source_updated_time
    from source
    )
    select * from tfm

