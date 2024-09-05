with
    d365_source as (
        select *
        from {{ source("D365", "invent_trans") }}  where _FIVETRAN_DELETED='FALSE' and  upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}
 
    ),

    renamed as (

        select
            'D365' as source,
            item_id as itemid,
            status_issue as statusissue,
            INTER_COMPANY_INVENT_DIM_TRANSFERRED as intercoinventdimtransferred,
            date_physical as datephysical,
            qty as qty,
            cost_amount_posted as costamountposted,
            currency_code as currencycode,
            invoice_id as invoiceid,
            voucher as voucher,
            date_expected as dateexpected,
            date_financial as datefinancial,
            cost_amount_physical as costamountphysical,
            status_receipt as statusreceipt,
            packing_slip_returned as packingslipreturned,
            invoice_returned as invoicereturned,
            packing_slip_id as packingslipid,
            voucher_physical as voucherphysical,
            cost_amount_adjustment as costamountadjustment,
            shipping_date_requested as shippingdaterequested,
            shipping_date_confirmed as shippingdateconfirmed,
            qty_settled as qtysettled,
            cost_amount_settled as costamountsettled,
            value_open as valueopen,
            null as activitynumber,
            date_status as datestatus,
            cost_amount_std as costamountstd,
            date_closed as dateclosed,
            picking_route_id as pickingrouteid,
            cost_amount_operations as costamountoperations,
            return_invent_trans_origin as returninventtransorigin,
            null as projid,
            null as projcategoryid,
            invent_dim_id as inventdimid,
            marking_ref_invent_trans_origin as markingrefinventtransorigin,
            invent_dim_fixed as inventdimfixed,
            date_invent as dateinvent,
            trans_child_ref_id as transchildrefid,
            trans_child_type as transchildtype,
            time_expected as timeexpected,
            revenue_amount_physical as revenueamountphysical,
            null as projadjustrefid,
            tax_amount_physical as taxamountphysical,
            invent_trans_origin as inventtransorigin,
            storno_ru as storno_ru,
            storno_physical_ru as stornophysical_ru,
            null as inventdimidsales_ru,
            group_ref_type_ru as groupreftype_ru,
            null as grouprefid_ru,
            cost_amount_sec_cur_posted_ru as costamountseccurposted_ru,
            cost_amount_sec_cur_physical_ru as costamountseccurphysical_ru,
            cost_amount_sec_cur_adjustment_ru as costamountseccuradjustment_ru,
            date_closed_sec_cur_ru as dateclosedseccur_ru,
            qty_settled_sec_cur_ru as qtysettledseccur_ru,
            cost_amount_settled_sec_cur_ru as costamountsettledseccur_ru,
            value_open_sec_cur_ru as valueopenseccur_ru,
            cost_amount_std_sec_cur_ru as costamountstdseccur_ru,
            invent_trans_origin_delivery_ru as inventtransorigindelivery_ru,
            invent_trans_origin_sales_ru as inventtransoriginsales_ru,
            invent_trans_origin_transit_ru as inventtransorigintransit_ru,
            pds_cwqty as pdscwqty,
            pds_cwsettled as pdscwsettled,
            NON_FINANCIAL_TRANSFER_INVENT_CLOSING as nonfinancialtransferinventclos,
            modifieddatetime as modifieddatetime,
            upper(data_area_id) as dataareaid,
            recversion as recversion,
            partition as partition,
            recid as recid,
            null as whsuser,
            null as modifiedby,
            null as createddatetime

        from d365_source

    )

select * from renamed
