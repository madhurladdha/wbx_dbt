
with d365_source as (
        select *
        from {{ source("D365", "cust_packing_slip_jour") }}
        where trim(upper(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE' 
        
    ),

renamed as (

   
    select 
        'D365' as source,
        ref_num as refnum,
        sales_id as salesid,
        order_account as orderaccount,
        invoice_account as invoiceaccount,
        packing_slip_id as packingslipid,
        purchase_order as purchaseorder,
        delivery_date as deliverydate,
        delivery_name as deliveryname,
        qty as qty,
        volume as volume,
        weight as weight,
        printed as printed,
        inter_company_posted as intercompanyposted,
        null as intrastatdispatch,
        dlv_term as dlvterm,
        dlv_mode as dlvmode,
        print_mgmt_site_id as printmgmtsiteid,
        ledger_voucher as ledgervoucher,
        return_item_num as returnitemnum,
        sales_type as salestype,
        freight_slip_type as freightsliptype,
        null as freightslipnum,
        parm_id as parmid,
        list_code as listcode,
        customer_ref as customerref,
        language_id as languageid,
        invent_location_id as inventlocationid,
        null as billofladingid,
        null as exportreason,
        document_date as documentdate,
        null as numbersequencegroup,
        null as intercompanycompanyid,
        null as intercompanypurchid,
        null as bolpackageappearance,
        null as bolcarriername,
        null as boladdress,
        invoicing_name as invoicingname,
        null as dlvreason,
        bolfreighted_by as bolfreightedby,
        return_packing_slip_id as returnpackingslipid,
        null as shipcarrierdeliverycontact,
        null as shipcarrieraccount,
        null as shipcarrierid,
        ship_carrier_blind_shipment as shipcarrierblindshipment,
        null as shipcarrierphone,
        null as shipcarrieremail,
        delivery_postal_address  as deliverypostaladdress,
        invoice_postal_address   as invoicepostaladdress,
        default_dimension  as defaultdimension,
        worker_sales_taker  as workersalestaker,
        source_document_header  as sourcedocumentheader,
        internal_packing_slip_id as internalpackingslipid,
        compiler as compiler,
        transportation_delivery_loader as transportationdeliveryloader,
        transportation_delivery_owner as transportationdeliveryowner,
        transportation_delivery_contractor as transportationdeliverycontractor,
        intrastat_fulfillment_date_hu as intrastatfulfillmentdate_hu,
        invent_profile_type_ru as inventprofiletype_ru,
        null as packingslipregister_lt,
        null as packingslipnumberingcode_lt,
        null as packingslipstatus_lt,
        print_blank_date_lt as printblankdate_lt,
        null as contactpersonid,
        invoice_issue_due_date_w as invoiceissueduedate_w,
        null as offsessionid_ru,
        pds_cwqty as pdscwqty,
        reason_table_ref_br as reasontableref_br,
        transportation_document as transportationdocument,
        bank_lcexport_line as banklcexportline,
        createddatetime as createddatetime,
        null as del_createdtime,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as bisediprocess
    from d365_source

)

select * from renamed
