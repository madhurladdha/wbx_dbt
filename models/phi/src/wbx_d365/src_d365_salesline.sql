with d365_source as (
        select *
        from {{ source("D365", "sales_line") }}  where _FIVETRAN_DELETED='FALSE' and upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}}

    ),

    renamed as (

        
        select
            'D365' as source,
            sales_id as salesid,
            cast(line_num as decimal(15,2)) linenum,
            item_id as itemid,
            sales_status as salesstatus,
            name as name,
            external_item_id as externalitemid,
            tax_group as taxgroup,
            qty_ordered as qtyordered,
            sales_deliver_now as salesdelivernow,
            remain_sales_physical as remainsalesphysical,
            remain_sales_financial as remainsalesfinancial,
            cost_price as costprice,
            sales_price as salesprice,
            currency_code as currencycode,
            line_percent as linepercent,
            line_disc as linedisc,
            line_amount as lineamount,
            confirmed_dlv as confirmeddlv,
            reservation as reservation,
            null as salesgroup,
            sales_unit as salesunit,
            price_unit as priceunit,
            null as projtransid,
            invent_trans_id as inventtransid,
            cust_group as custgroup,
            cust_account as custaccount,
            sales_qty as salesqty,
            sales_markup as salesmarkup,
            invent_deliver_now as inventdelivernow,
            multi_ln_disc as multilndisc,
            multi_ln_percent as multilnpercent,
            sales_type as salestype,
            blocked as blocked,
            complete as complete,
            remain_invent_physical as remaininventphysical,
            null as transactioncode,
            null as countyorigdest,
            tax_item_group as taxitemgroup,
            tax_autogenerated as taxautogenerated,
            under_delivery_pct as underdeliverypct,
            over_delivery_pct as overdeliverypct,
            bar_code as barcode,
            bar_code_type as barcodetype,
            invent_ref_trans_id as inventreftransid,
            invent_ref_type as inventreftype,
            invent_ref_id as inventrefid,
            inter_company_origin as intercompanyorigin,
            null as itembomid,
            null as itemrouteid,
            null as lineheader,
            scrap as scrap,
            dlv_mode as dlvmode,
            invent_trans_id_return as inventtransidreturn,
            null as projcategoryid,
            null as projid,
            invent_dim_id as inventdimid,
            null as transport,
            null as statprocid,
            null as port,
            null as projlinepropertyid,
            receipt_date_requested as receiptdaterequested,
            customer_line_num as customerlinenum,
            packing_unit_qty as packingunitqty,
            packing_unit as packingunit,
            inter_company_invent_trans_id as intercompanyinventtransid,
            remain_invent_financial as remaininventfinancial,
            delivery_name as deliveryname,
            delivery_type as deliverytype,
            customer_ref as customerref,
            purchorder_form_num as purchorderformnum,
            receipt_date_confirmed as receiptdateconfirmed,
            stat_triangular_deal as stattriangulardeal,
            shipping_date_requested as shippingdaterequested,
            shipping_date_confirmed as shippingdateconfirmed,
            address_ref_rec_id as addressrefrecid,
            address_ref_table_id as addressreftableid,
            null as serviceorderid,
            itemtagging as itemtagging,
            casetagging as casetagging,
            pallettagging as pallettagging,
            line_delivery_type as linedeliverytype,
            null as einvoiceaccountcode,
            null as shipcarrierid,
            null as shipcarrieraccount,
            ship_carrier_dlv_type as shipcarrierdlvtype,
            null as shipcarrieraccountcode,
            sales_category as salescategory,
            delivery_date_control_type as deliverydatecontroltype,
            null as activitynumber,
            ledger_dimension as ledgerdimension,
            return_allow_reservation as returnallowreservation,
            matching_agreement_line as matchingagreementline,
            system_entry_source as systementrysource,
            system_entry_change_policy as systementrychangepolicy,
            manual_entry_changepolicy as manualentrychangepolicy,
            item_replaced as itemreplaced,
            return_deadline as returndeadline,
            expected_ret_qty as expectedretqty,
            return_status as returnstatus,
            return_arrival_date as returnarrivaldate,
            return_closed_date as returncloseddate,
            return_disposition_code_id as returndispositioncodeid,
            delivery_postal_address as deliverypostaladdress,
            ship_carrier_postal_address as shipcarrierpostaladdress,
            null as shipcarriername,
            default_dimension as defaultdimension,
            source_document_line as sourcedocumentline,
            tax_withhold_item_group_heading_th as taxwithholditemgroupheading_th,
            stocked_product as stockedproduct,
            null as customsname_mx,
            null as customsdocnumber_mx,
            customs_doc_date_mx as customsdocdate_mx,
            null as propertynumber_mx,
            null as itempbaid,
            ref_return_invoice_trans_w as refreturninvoicetrans_w,
            null as postingprofile_ru,
            null as taxwithholdgroup,
            intrastat_fulfillment_date_hu as intrastatfulfillmentdate_hu,
            null as assetid_ru,
            statistic_value_lt as statisticvalue_lt,
            credit_note_internal_ref_pl as creditnoteinternalref_pl,
            psaproj_proposal_qty as psaprojproposalqty,
            psaproj_proposal_invent_qty as psaprojproposalinventqty,
            pds_exclude_from_rebate as pdsexcludefromrebate,
            retail_variant_id as retailvariantid,
            agreement_skip_auto_link as agreementskipautolink,
            null as countryregionname_ru,
            credit_note_reason_code as creditnotereasoncode,
            null as deliverytaxgroup_br,
            null as deliverytaxitemgroup_br,
            dlv_term as dlvterm,
            null as invoicegtdid_ru,
            mcrorder_line_2_price_history_ref as mcrorderline2pricehistoryref,
            pds_batch_attrib_auto_res as pdsbatchattribautores,
            pds_cwexpected_ret_qty as pdscwexpectedretqty,
            pds_cwinvent_deliver_now as pdscwinventdelivernow,
            pds_cwqty as pdscwqty,
            pds_cwremain_invent_financial as pdscwremaininventfinancial,
            pds_cwremain_invent_physical as pdscwremaininventphysical,
            null as pdsitemrebategroupid,
            pds_same_lot as pdssamelot,
            pds_same_lot_override as pdssamelotoverride,
            price_agreement_date_ru as priceagreementdate_ru,
            null as psacontractlinenum,
            retail_block_qty as retailblockqty,
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
            null as wbxshelflife,
            null as wbxvariantmemo,
            null as orderlinereference_no,
            null as satunitcode_mx,
            null as satproductcode_mx

        from d365_source

    )

select * from renamed 


