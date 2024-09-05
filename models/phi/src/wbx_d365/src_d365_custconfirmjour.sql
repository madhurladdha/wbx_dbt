
with

    d365_source as (
        select * from {{ source("D365", "cust_confirm_jour") }}  where upper(data_area_id) in {{env_var("DBT_D365_COMPANY_FILTER")}} and _FIVETRAN_DELETED='FALSE' 
    ),
renamed as (
 
    select
        'D365' as source,
        confirm_id as confirmid,
        confirm_date as confirmdate,
        sales_id as salesid,
        order_account as orderaccount,
        invoice_account as invoiceaccount,
        cust_group as custgroup,
        purchase_order as purchaseorder,
        delivery_name as deliveryname,
        dlv_term as dlvterm,
        dlv_mode as dlvmode,
        payment as payment,
        null as cashdisccode,
        cash_disc_percent as cashdiscpercent,
        inter_company_posted as intercompanyposted,
        qty as qty,
        volume as volume,
        weight as weight,
        cost_value as costvalue,
        sum_line_disc as sumlinedisc,
        sales_balance as salesbalance,
        sum_markup as summarkup,
        end_disc as enddisc,
        round_off as roundoff,
        confirm_amount as confirmamount,
        currency_code as currencycode,
        exch_rate as exchrate,
        sum_tax as sumtax,
        parm_id as parmid,
        confirm_doc_num as confirmdocnum,
        exch_rate_secondary as exchratesecondary,
        triangulation as triangulation,
        customer_ref as customerref,
        language_id as languageid,
        incl_tax as incltax,
        fixed_due_date as fixedduedate,
        deadline as deadline,
        delivery_postal_address  as deliverypostaladdress,
        default_dimension  as defaultdimension,
        worker_sales_taker  as workersalestaker,
        customs_export_order_in as customsexportorder_in,
        createddatetime as createddatetime,
        upper(data_area_id) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as bisediprocess
    from d365_source

)

select * from renamed

