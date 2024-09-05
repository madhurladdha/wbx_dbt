{{
    config (
        materialized = env_var('DBT_MAT_INCREMENTAL'),
        transient = false,
        unique_key = 'UNIQUE_KEY',
        on_schema_change='sync_all_columns'
    )
}}

with supplier_source as (
    select * from {{ ref('dim_wbx_supplier') }} where source_system='{{env_var("DBT_SOURCE_SYSTEM")}}'
    and source_system_address_number in (
    select cast(source_supplier_identifier as varchar(255)) from (
----ap voucher	
    select distinct cast(vnd.accountnum as varchar(255)) as source_supplier_identifier from
    (select upper(trim(v.dataareaid)) as dataareaid,v.voucher as voucher,max(v.invoice) as invoice,max(v.accountnum) as accountnum,max(v.transtype) as transtype
        from {{ ref("src_vendtrans") }} v group by upper(trim(v.dataareaid)), v.voucher) vnd
    inner join
    {{ ref("src_vendinvoicetrans") }} vit on vnd.invoice = vit.invoiceid and upper(trim(vnd.dataareaid)) = upper(trim(vit.dataareaid))
	where trim(vnd.invoice) is not null and vnd.transtype not in (9, 15, 24)

    union 

    --po fact
    select distinct cast(pt.invoiceaccount as varchar(255)) as source_supplier_identifier
    from {{ ref("src_purchtable") }} pt
    inner join {{ ref("src_purchline") }} pl
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid)) and pt.purchid = pl.purchid
    inner join {{ ref("src_inventdim") }} id
    on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid))
    and pl.inventdimid = id.inventdimid
    and upper(trim(id.dataareaid)) = upper(trim(pt.dataareaid))
	
union

--po_receipt_fact
    select distinct cast(pt.invoiceaccount as varchar(255)) as source_supplier_identifier
    from {{ ref("src_purchtable") }} pt
    inner join {{ ref("src_purchline") }} pl on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid)) and pt.purchid = pl.purchid
    inner join {{ ref("src_inventdim") }} id on upper(trim(pt.dataareaid)) = upper(trim(pl.dataareaid)) and pl.inventdimid = id.inventdimid 
    and upper(trim(id.dataareaid)) = upper(trim(pt.dataareaid))	
inner join 
		(select distinct ppl.origpurchid,ppl.purchaselinelinenumber from {{ ref("src_purchparmtable") }} ppt
        inner join {{ ref("src_purchparmline") }} ppl on ppt.tablerefid = ppl.tablerefid and ppt.parmid = ppl.parmid and ppt.enddatetime <> '1900-01-01') rcpts	
		on pl.purchid = rcpts.origpurchid
		and pl.linenumber = rcpts.purchaselinelinenumber)	
    )        		

    /* Used above part as filter out records, in iics code this part was refering FIN_AP_VOUCHER_FACT,PRC_PO_FACT & PRC_PO_RECEIPT_FACT.
    Used source tables instead to aviod circular logic in dbt */
   
),
old_supplier_source as (
    select * from {{ ref('conv_dim_wbx_supplier_category') }}
),

new_dim as (
select 
A.UNIQUE_KEY,
A.SUPPLIER_ADDRESS_NUMBER_GUID,
A.SOURCE_SYSTEM,
A.SOURCE_SYSTEM_ADDRESS_NUMBER,
A.COMPANY_CODE,
A.SUPPLIER_NAME,
CAST (SYSTIMESTAMP() AS VARCHAR2 (255)) as UPDATE_DATE,
NULL AS UPDATED_BY
FROM supplier_source A left join old_supplier_source B 
on A.UNIQUE_KEY=B.UNIQUE_KEY
),


old_dim as (
select 
A.UNIQUE_KEY,
A.SUPPLIER_ADDRESS_NUMBER_GUID,
A.SOURCE_SYSTEM,
A.SOURCE_SYSTEM_ADDRESS_NUMBER,
A.COMPANY_CODE,
A.SUPPLIER_NAME,
A.UPDATE_DATE,
A.UPDATED_BY
FROM old_supplier_source A left join supplier_source B 
on A.UNIQUE_KEY=B.UNIQUE_KEY
WHERE B.UNIQUE_KEY IS NULL
),
final_dim as (

select * from new_dim
union 
select * from old_dim
),

final_fields as (
    select distinct
        UNIQUE_KEY,
        SUPPLIER_ADDRESS_NUMBER_GUID,
        CAST (SOURCE_SYSTEM   				AS VARCHAR2 (255)) 		AS SOURCE_SYSTEM,
        CAST (SOURCE_SYSTEM_ADDRESS_NUMBER 	AS VARCHAR2 (255)) 		AS SOURCE_SYSTEM_ADDRESS_NUMBER,
        CAST (SUPPLIER_NAME	            	AS VARCHAR2 (255)) 		AS SUPPLIER_NAME,
        CAST (UPDATE_DATE                   AS VARCHAR2 (255)) 		AS UPDATE_DATE,
        CAST (UPDATED_BY                    AS VARCHAR2 (255)) 		AS UPDATED_BY,
        CAST (COMPANY_CODE                  AS VARCHAR2 (255)) 	    AS COMPANY_CODE
        FROM final_dim
)

select * from final_fields
