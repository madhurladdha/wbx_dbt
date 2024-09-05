{{
    config(
        tags=["manufacturing","agreement","wbx",],
        )

}}
with
    agreementheader as (select * from {{ ref("src_agreementheader") }}),
    agreementline as (select * from {{ ref("src_agreementline") }}),
    agreementclassification as (select * from {{ ref("src_agreementclassification") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    agreementlinereleasedline as (select * from {{ ref("src_agreementlinereleasedline") }}),
    purchline as (select * from {{ ref("src_purchline") }}),
    vendinvoicetrans as (select * from {{ ref("src_vendinvoicetrans") }}),

    source as (
        select
            '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system,
            pa.purchnumbersequence as agreement_number,
            pal.linenumber as line_number,
            ac.name as agreement_type_desc,
            (pa.vendordataareaid) as source_company,
            id.inventlocationid as source_business_unit_code,
            pal.itemid as source_item_identifier,
            id.inventsizeid as variant_code,
            id.inventsiteid as site_code,
            pa.agreementstate as status_code,
            case
                pa.agreementstate when 0 then 'On hold' when 1 then 'Effective'
            end as status_desc,
            pa.workflowstatus_psn as approval_status_code,
            case
                pa.workflowstatus_psn
                when 1
                then 'Draft'
                when 2
                then 'Submitted'
                when 4
                then 'Approved'
                when 3
                then 'In Review'
                when 10
                then 'Not Submitted'
                when 5
                then 'Rejected'
                when 6
                then 'Change requested'
                when 8
                then 'Pending'
                when 9
                then 'Cancellation'
                when 11
                then 'Pending completion'
                when 12
                then 'Completed'
                when 13
                then 'Pending cancellation'
                when 14
                then 'None'
                when 15
                then 'Returned'
            end as approval_status_desc,
            pal.effectivedate as agreement_eff_date,
            pal.expirationdate as agreement_exp_date,
            pa.vendaccount as supplier_address_number,
            pal.commitedquantity as agreement_quantity,
            pal.commitedquantity as original_quantity,
            pal.priceperunit as price_per_unit,
            productunitofmeasure as unit_of_measure,
            pal.priceunit as price_unit,
            pal.currency as currency_code,
            pa.isdeleted as deleted_flag,
            sum(
                case
                    nvl(pl.purchstatus, -1) when 1 then pl.remainpurchphysical else 0
                end
            ) as released_quantity,
            sum(nvl(pl.remainpurchfinancial, 0)) as received_quantity,
            sum(nvl(vit.qty, 0)) as invoiced_quantity,
            trunc(current_timestamp(),'DD')  source_update_date,
            current_timestamp()  source_updated_time
        from agreementheader pa
        inner join
            agreementline pal
            on pa.partition = pal.partition
            and pa.recid = pal.agreement
        inner join
            agreementclassification ac
            on pa.agreementclassification = ac.recid
            and pa.partition = ac.partition
        inner join
            inventdim id
            on pal.partition = id.partition
            and pal.inventdimdataareaid = id.dataareaid
            and pal.inventdimid = id.inventdimid
        left outer join
            agreementlinereleasedline palrl
            on pal.partition = palrl.partition
            and pal.recid = palrl.agreementline
        left outer join
            purchline pl
            on palrl.purchlineinventtransid = pl.inventtransid
            and pl.purchstatus in (1, 2)
        left outer join
            vendinvoicetrans vit on palrl.vendinvoicetrans = vit.recid
        group by
            pa.purchnumbersequence,
            pal.linenumber,
            ac.name,
            pa.vendordataareaid,
            id.inventlocationid,
            pal.itemid,
            id.inventsizeid,
            id.inventsiteid,
            pa.agreementstate,
            pa.agreementstate,
            pa.workflowstatus_psn,
            pa.workflowstatus_psn,
            pal.effectivedate,
            pal.expirationdate,
            pa.vendaccount,
            pal.commitedquantity,
            pal.commitedquantity,
            pal.priceperunit,
            productunitofmeasure,
            pal.priceunit,
            pal.currency,
            pa.isdeleted,
            source_update_date,
            source_updated_time

    )

select *
from source
