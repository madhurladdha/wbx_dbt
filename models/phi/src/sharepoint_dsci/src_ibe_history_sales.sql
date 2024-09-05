/*
To DO: This model is pointing to a sample test file. Once we get production file, change file name in line no.6. Upstream model
should run fine and no changes needed in that
*/

with source as (
    select * from {{ source('SHAREPOINT_DSCI', 'ibe_history_sales') }}
),

renamed as (

    select
        _line,
        _fivetran_synced,
        sales_order,
        sales_order_line,
        order_type,
        order_status,
        payment_terms,
        customer_account,
        sku,
        site,
        date_ordered,
        date_invoiced,
        date_despatched,
        date_gl,
        date_cancelled,
        trans_unit_of_measure,
        qty_ordered,
        qty_open,
        qty_short,
        qty_despatched,
        qty_cancelled,
        trans_currency,
        amt_invoice_gross,
        amt_invoice_discount,
        amt_invoice_net
    from source

)

select * from renamed
