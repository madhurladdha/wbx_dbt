{{ config(
    snowflake_warehouse=env_var("DBT_WBX_SF_WH"), 
    tags=["pricing"]
    ) 
}}

/*  Mike Traub 5/16/2024 - summary of adjustements for D365 (BR1):
    -Customer customer extension (wbxcusttableext) does not exist in D365.  So removed.
    -The attributes for OIF (Off-Invoice Funding) and ACG (Tiered price list) need replacement based on the previous statement.
    -Per Dave Le Gros, ACG will not be used or applicable in D365.  So those references will be null-ed out or deleted.
    -Changes (removals) of code and joins are throughout to suppress the ACG aspect while retaining the same structure.
    -Category - the case logic for the Category field is working with the small set of data currently in pricedisctable.  Not sure if all of those will 
        continue to work with more data and/or when main-site data is added for BR2.
*/
with wbxcustomerallowedproducts as (
    select * from {{ ref("src_wbxcustomer_allowed_list") }}
),
    pricedisctable as (
        select * from {{ ref('src_pricedisctable')}}
    ),
    custtable as (
        select * from {{ ref('src_custtable')}}
    ),
    fiscalcalendarperiod as (
        select * from  {{ ref('src_fiscalcalendarperiod')}}
    ),
    /* Not a table in D365  *********************/
    -- wbxcusttableext as (
    --     select * from {{ ref('src_wbxcusttableext')}}
    -- ),
    /********************************************/
    fiscalcalendaryear as (
        select * from {{ ref('src_fiscalcalendaryear')}}
    ),
    fiscalcalendar as (
        select * from {{ ref('src_fiscalcalendar')}}
    ),
    account_hierarchy as (
        select * from {{ ref('v_wtx_account_hierarchy')}}
    ),
    product_hierarchy as (
        select * from {{ ref('v_wtx_product_hierarchy')}}
    ),
    dim_date as (
        select * from {{ref('src_dim_date')}} 
    ),
    custallowed as (
        select distinct
            a.accountnum,
            a.itemid,
            case
                when a.fromdate < nvl(b.fromdate, date('2154-12-31'))
                then a.fromdate
                else b.fromdate
            end as fromdate,
            case
                when a.todate > nvl(b.todate, date('1900-01-01'))
                then a.todate
                else b.todate
            end as todate
        from wbxcustomerallowedproducts a
        left join wbxcustomerallowedproducts b
            on a.accountnum = b.accountnum
            and a.itemid = b.itemid
            and (b.fromdate <= a.todate)
            and (b.todate >= a.fromdate)
            and a.recid <> b.recid
        where (upper(trim(a.dataareaid))) in {{ env_var("DBT_COMPANY_FILTER") }} and a.todate >= current_date()
    ),
    dt as (
        select
            fcp.enddate as periodenddate,
            fcp.month + 1 as periodno,
            fcp.quarter + 1 as quarterno,
            fcp.startdate as periodstartdate,
            fcp.name as periodname,
            fcy.name as yearname,
            fc.description
        from fiscalcalendarperiod fcp
        inner join fiscalcalendaryear fcy on fcp.fiscalcalendaryear = fcy.recid
        inner join fiscalcalendar fc on fcp.fiscalcalendar = fc.recid
        where fcp.type = 1 and fc.calendarid = 'WBX PostH'
    ),
    pricebranchfg as (
        select distinct accountrelation, itemrelation
        from pricedisctable
        where
            (upper(trim(dataareaid)) in {{ env_var("DBT_COMPANY_FILTER") }})
            and (relation = 4)
            and (accountcode in (0))
            and itemrelation <> ''
    ),
    pricegroupfg as (
        select distinct accountrelation, itemrelation
        from pricedisctable
        where
            (upper(trim(dataareaid)) in {{ env_var("DBT_COMPANY_FILTER") }})
            and (relation = 4)
            and (accountcode in (1))
            and itemrelation <> ''
    ),
    custdt as (
        select
            accountnum,
            pricegroup,
            itemrelation,
            ct.linedisc as oifgroup,    --wct.additionaldisc as oifgroup,
            null as acggroup    --wct.totalsoqtydisc as acggroup
        from custtable ct
        -- inner join wbxcusttableext wct
        --     on upper(trim(ct.dataareaid)) = upper(trim(wct.dataareaid))
        --     and ct.partition = wct.partition
        --     and ct.accountnum = wct.custaccount
        inner join pricebranchfg on ct.accountnum = pricebranchfg.accountrelation
        where upper(trim(ct.dataareaid)) in {{ env_var("DBT_COMPANY_FILTER") }} and ct.pricegroup = ''
        union
        select
            accountnum,
            pricegroup,
            itemrelation,
            ct.linedisc as oifgroup,    --wct.additionaldisc as oifgroup,
            null as acggroup    --wct.totalsoqtydisc as acggroup
        from custtable ct
        -- inner join wbxcusttableext  wct
        --     on upper(trim(ct.dataareaid)) = upper(trim(wct.dataareaid))
        --     and ct.partition = wct.partition
        --     and ct.accountnum = wct.custaccount
        inner join pricegroupfg on ct.pricegroup = pricegroupfg.accountrelation
        where upper(trim(ct.dataareaid)) in {{ env_var("DBT_COMPANY_FILTER") }} and ct.pricegroup <> ''
    ),
    custdt_date as (
        select
            accountnum, pricegroup, itemrelation, dr.calendar_date, oifgroup, acggroup
        from custdt ct
        cross join dim_date dr
        where
            (
                dr.calendar_date between dateadd(month, -3, current_date()) and dateadd(
                    month, 9, current_date()
                )
            )
            and (
                dr.calendar_date = current_date()
                or dr.calendar_date = current_date() - 1
                or dr.calendar_date = current_date() + 1 
                or day(dr.calendar_date) = 1
            )
            and right(accountnum, 4) <> '0000'
    ),
    pricegrouping as (
        select
            recid,
            itemrelation,
            accountrelation,
            quantityamountfrom,
            fromdate,
            case
                todate when '1900-01-01' then date('2154-12-31') else todate
            end as todate,
            amount,
            currency,
            wbxfixedexchangerate,
            case
                when relation = 4 and accountcode = 1
                then 'GroupPrices'
                when relation = 249 and accountcode = 1 and quantityamountfrom = 350
                then 'GroupCPrices'
                when relation = 249 and accountcode = 1 and quantityamountfrom <> 350
                then 'GroupGPrices'
                when relation = 250 and accountcode = 1
                then 'GroupOIF'
                when relation = 4 and accountcode = 0
                then 'BranchPrices'
                when relation = 249 and accountcode = 0 and quantityamountfrom = 350
                then 'BranchCPrices'
                when relation = 249 and accountcode = 0 and quantityamountfrom <> 350
                then 'BranchGPrices'
                when relation = 250 and accountcode = 0
                then 'BranchOIF'
            end as category
        from pricedisctable
        where
            (upper(trim(dataareaid)) in {{ env_var("DBT_COMPANY_FILTER") }})
            and itemrelation <> ''
            and relation in (4, 249, 250)
            and accountcode in (1, 0)
    ),
    caldatelevel_results as (

        select
            cd.*,
            coalesce(
                nullif(cd.pricegroup, ''),
                concat(
                    acct.trade_type,
                    ': ',
                    acct.customer_branch_number,
                    ' - ',
                    acct.customer_branch
                )
            ) as group_acct_desc,
            case nvl(ca.itemid, '00000') when '00000' then 0 else 1 end as allowed,
            /* Removing ACG from these coalesce statements. */
            coalesce(
                gr.quantityamountfrom,
                gri.quantityamountfrom,
                oif.quantityamountfrom
            ) as quantityamountfrom,
            coalesce(gr.fromdate, gri.fromdate, oif.fromdate) as fromdate,
            coalesce(gr.todate, gri.todate, oif.todate) as todate,
            coalesce(gr.amount, gri.amount, oif.amount) as amount,
            coalesce(gr.currency, gri.currency, oif.currency) as currency,
            coalesce(
                gr.wbxfixedexchangerate,
                gri.wbxfixedexchangerate,
                oif.wbxfixedexchangerate
            ) as wbxfixedexchangerate,
            coalesce(gr.category, gri.category, oif.category) as category,
            coalesce(gr.recid, gri.recid, oif.recid) as recid,
            acct.market_code,
            acct.market,
            acct.sub_market_code,
            acct.sub_market,
            acct.trade_class_code,
            acct.trade_class,
            acct.trade_group_code,
            acct.trade_group,
            acct.trade_type_code,
            acct.trade_type,
            acct.customer_account_number,
            acct.customer_account,
            acct.customer_branch_number,
            acct.customer_branch,
            acct.market_seq,
            acct.sub_market_seq,
            acct.trade_class_seq,
            acct.trade_group_seq,
            acct.trade_type_seq,
            prod.branding_code,
            prod.branding,
            prod.product_code,
            prod.product_class,
            prod.sub_product_code,
            prod.sub_product,
            prod.item_sku,
            prod.item_desc,
            prod.pack_size_desc,
            prod.branding_seq,
            prod.product_class_seq,
            prod.sub_product_seq,
            prod.pack_size_seq,
            prod.net_weight,
            prod.tare_weight,
            prod.avp_weight,
            prod.avp_flag,
            prod.pmp_flag,
            prod.consumer_units_in_trade_units,
            prod.consumer_units,
            prod.pallet_qty,
            prod.current_flag,
            prod.gross_weight,
            prod.gross_depth,
            prod.gross_width,
            prod.gross_height,
            prod.pallet_qty_per_layer,
            prod.exclude_indicator,
            prod.fin_dim_product,
            prod.fin_dim_product_desc,
            prod.filter_code1,
            prod.filter_desc1,
            prod.filter_code2,
            prod.filter_desc2,
            prod.pallet_type,
            prod.pallet_config,
            prod.stock_type,
            prod.stock_desc,
            prod.item_type,
            prod.primary_uom,
            prod.primary_uom_desc,
            prod.obsolete_flag_ax,
            prod.tuc,
            prod.dun,
            prod.ean13

        from custdt_date cd
        left outer join custallowed ca
            on cd.accountnum = ca.accountnum
            and cd.itemrelation = ca.itemid
            and cd.calendar_date between ca.fromdate and ca.todate
        left join pricegrouping gr
            on cd.itemrelation = gr.itemrelation
            and cd.pricegroup = gr.accountrelation
            and cd.calendar_date between gr.fromdate and gr.todate
        left join pricegrouping gri
            on cd.itemrelation = gri.itemrelation
            and cd.accountnum = gri.accountrelation
            and cd.calendar_date between gri.fromdate and gri.todate
        left join pricegrouping oif
            on cd.itemrelation = oif.itemrelation
            and cd.oifgroup = oif.accountrelation
            and cd.calendar_date between oif.fromdate and oif.todate
        /*ACG not applicable in D365    */
        -- left join pricegrouping acg
        --     on cd.itemrelation = acg.itemrelation
        --     and cd.acggroup = acg.accountrelation
        --     and cd.calendar_date between acg.fromdate and acg.todate
        left join account_hierarchy acct
            on cd.accountnum = acct.customer_branch_number
        left join product_hierarchy prod 
            on cd.itemrelation = prod.item_sku
    ),
final as (
select distinct * from caldatelevel_results a
where
    a.recid = (
        select min(a1.recid)
        from caldatelevel_results a1
        where
            a.accountnum = a1.accountnum
            and a.pricegroup = a1.pricegroup
            and a.itemrelation = a1.itemrelation
            and a.calendar_date = a1.calendar_date
            and nvl(a.oifgroup,'-') = nvl(a1.oifgroup,'-')
            --and a.acggroup = a1.acggroup
            and a.allowed = a1.allowed
            and a.currency = a1.currency
            and a.category = a1.category
            and a.quantityamountfrom = a1.quantityamountfrom)
    )
select
    accountnum,
        pricegroup,
        itemrelation,
        calendar_date,
        oifgroup,
        acggroup,
        group_acct_desc,
        allowed,
        quantityamountfrom,
        fromdate,
        todate,
        amount,
        currency,
        wbxfixedexchangerate,
        category,
        recid,
        market_code,
        market,
        sub_market_code,
        sub_market,
        trade_class_code,
        trade_class,
        trade_group_code,
        trade_group,
        trade_type_code,
        trade_type,
        customer_account_number,
        customer_account,
        customer_branch_number,
        customer_branch,
        market_seq,
        sub_market_seq,
        trade_class_seq,
        trade_group_seq,
        trade_type_seq,
        branding_code,
        branding,
        product_code,
        product_class,
        sub_product_code,
        sub_product,
        item_sku,
        item_desc,
        pack_size_desc,
        branding_seq,
        product_class_seq,
        sub_product_seq,
        pack_size_seq,
        net_weight,
        tare_weight,
        avp_weight,
        avp_flag,
        pmp_flag,
        consumer_units_in_trade_units,
        consumer_units,
        pallet_qty,
        current_flag,
        gross_weight,
        gross_depth,
        gross_width,
        gross_height,
        pallet_qty_per_layer,
        exclude_indicator,
        fin_dim_product,
        fin_dim_product_desc,
        filter_code1,
        filter_desc1,
        filter_code2,
        filter_desc2,
        pallet_type,
        pallet_config,
        stock_type,
        stock_desc,
        item_type,
        primary_uom,
        primary_uom_desc,
        obsolete_flag_ax,
        tuc,
        dun,
        ean13
    from final
