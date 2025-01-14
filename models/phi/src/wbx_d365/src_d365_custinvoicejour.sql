
with d365_source as (
        select *
        from {{ source("D365", "cust_invoice_jour") }}
        where trim(upper(data_area_id)) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE' 
    ),
renamed as (
    select 
        'D365' as source,
        cust_group as custgroup,
        ref_num as refnum,
        sales_id as salesid,
        order_account as orderaccount,
        invoice_account as invoiceaccount,
        invoice_date as invoicedate,
        due_date as duedate,
        cash_disc as cashdisc,
        cash_disc_date as cashdiscdate,
        qty as qty,
        volume as volume,
        weight as weight,
        sum_line_disc as sumlinedisc,
        sales_balance as salesbalance,
        end_disc as enddisc,
        invoice_amount as invoiceamount,
        currency_code as currencycode,
        exch_rate as exchrate,
        invoice_id as invoiceid,
        ledger_voucher as ledgervoucher,
        updated as updated,
        on_account_amount as onaccountamount,
        tax_print_on_invoice as taxprintoninvoice,
        listcode as listcode,
        null as documentnum,
        document_date as documentdate,
        cash_disc_percent as cashdiscpercent,
        null as intrastatdispatch,
        delivery_name as deliveryname,
        null as enterprisenumber,
        purchase_order as purchaseorder,
        dlv_term as dlvterm,
        dlv_mode as dlvmode,
        payment as payment,
        null as cashdisccode,
        invoice_round_off as invoiceroundoff,
        sum_markup as summarkup,
        cov_status as covstatus,
        return_item_num as returnitemnum,
        posting_profile as postingprofile,
        backorder as backorder,
        prepayment as prepayment,
        tax_group as taxgroup,
        null as taxitemgroup,
        tax_specify_by_line as taxspecifybyline,
        einvoice_line_specific as einvoicelinespecific,
        one_time_customer as onetimecustomer,
        null as paymentsched,
        sum_tax as sumtax,
        sales_type as salestype,
        null as einvoiceaccountcode,
        inter_company_posted as intercompanyposted,
        parm_id as parmid,
        return_reason_code_id as returnreasoncodeid,
        eusales_list as eusaleslist,
        exch_rate_secondary as exchratesecondary,
        triangulation as triangulation,
        customer_ref as customerref,
        vatnum as vatnum,
        null as numbersequencegroup,
        language_id as languageid,
        incl_tax as incltax,
        null as log,
        null as paymdayid,
        invoicing_name as invoicingname,
        giro_type as girotype,
        null as contactpersonid,
        sales_origin_id as salesoriginid,
        null as billofladingid,
        invent_location_id as inventlocationid,
        fixed_due_date as fixedduedate,
        invoice_amount_mst as invoiceamountmst,
        invoice_round_off_mst as invoiceroundoffmst,
        sum_markup_mst as summarkupmst,
        sum_line_disc_mst as sumlinediscmst,
        end_disc_mst as enddiscmst,
        sales_balance_mst as salesbalancemst,
        sum_tax_mst as sumtaxmst,
        print_mgmt_site_id as printmgmtsiteid,
        return_status as returnstatus,
        null as intercompanycompanyid,
        null as intercompanypurchid,
        printed_originals as printedoriginals,
        proforma as proforma,
        null as rcsaleslist_uk,
        null as reversecharge_uk,
         delivery_postal_address   as deliverypostaladdress,
        invoice_postal_address  as invoicepostaladdress,
        source_document_header  as sourcedocumentheader,
         default_dimension  as defaultdimension,
        bank_lcexport_line as banklcexportline,
        worker_sales_taker  as workersalestaker,
        reversed_rec_id as reversedrecid,
        receipt_date_confirmed_es as receiptdateconfirmed_es,
        null as paymid,
        null as taxinvoicesalesid,
        intrastat_fulfillment_date_hu as intrastatfulfillmentdate_hu,
        mcremail as mcremail,
        mcrpaym_amount as mcrpaymamount,
        mcrdue_amount as mcrdueamount,
        cash_disc_base_date as cashdiscbasedate,
        direct_debit_mandate as directdebitmandate,
        is_correction as iscorrection,
        reason_table_ref as reasontableref,
        source_document_line as sourcedocumentline,
        transportation_document as transportationdocument,
        modifieddatetime as modifieddatetime,
        createddatetime as createddatetime,
        null as del_createdtime,
        createdby as createdby,
        upper(trim(data_area_id))  as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as bisediprocess,
        provisional_assessment_in as provisionalassessment_in,
        null as invoiceidentification_in
    from d365_source

)
select * from renamed

