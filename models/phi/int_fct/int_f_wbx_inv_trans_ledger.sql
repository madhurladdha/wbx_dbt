{{ config(tags=["inventory", "trans_ledger"] ,
          materialized=env_var('DBT_MAT_TABLE'),
          transient=true,
          snowflake_warehouse= env_var("DBT_WBX_SF_WH")
 ) }}

with
    source_details as (select {{ dbt_utils.surrogate_key(['source_system','related_address_number',"'CUSTOMER_MAIN'",'document_company']) }} AS CUSTOMER_ADDRESS_NUMBER_GUID,* from {{ ref("stg_f_wbx_inv_trans_ledger") }}),
    dim_wbx_address as (select * from {{ ref("dim_wbx_address") }}),
    dim_wbx_company as (select * from {{ ref("dim_wbx_company") }}),
    dim_wbx_item as (select * from {{ ref("dim_wbx_item") }}),
	ref_effective_currency_dim as (select * from {{ ref("src_ref_effective_currency_dim") }}),
   


    joined_source as (

        select
           nvl(address_guid,'0') as address_guid,
			base_amt,
			BASE_CURRENCY, 
			business_unit_address_guid,
			document_company,
			document_number,
			document_type,
			gl_date,
			item_guid,
			line_number,
			load_date,
            update_date,
			location_guid,
			lot_guid,
			lot_status_code,
            lot_status_desc,
			original_document_company,
			original_document_number,
			original_document_type,
			nvl(original_line_number,0) as original_line_number,
			pallet_count,
			reason_code,
			reason_code_desc,
			nvl(related_address_number,'-') as related_address_number,
			remark_txt,
			source_business_unit_code,
			source_document_type,
			source_item_identifier,
			source_location_code,			
			source_lot_code,
			source_original_document_type,
			source_pallet_id,
			source_system,
			transaction_amt,
			transaction_currency,
			transaction_date,
			transaction_kg_qty,
            transaction_lb_qty,
			transaction_pri_uom_amt,
			transaction_pri_uom_qty,
			transaction_pri_uom_unit_cost,
			transaction_qty,
            transaction_uom,
            transaction_unit_cost,
            VARIANT,
            

            unique_key
        from
            (
                select
                   
                    related_address_number,
                    address_guid,

                    case
                        when ukp_normalized_doc.normalized_value is null
                        then source_document_type
                        else ukp_normalized_doc.normalized_value
                    end as v_document_type_normalized,
                    v_document_type_normalized as document_type,

                    case
                        when ukp_normalized_org_doc.normalized_value is null
                        then source_original_document_type
                        else ukp_normalized_org_doc.normalized_value
                    end as temp_original_document_type,

                    case
                        when temp_original_document_type is null
                        then '-'
                        else temp_original_document_type
                    end as original_document_type,

                    gl_date,
                    document_number,
                    original_document_number,
                    source_item_identifier,
                    src.item_guid,
                    src.document_company,
                    src.original_document_company,
                    src.original_line_number,
                    line_number,
                    source_location_code,
                    location_guid,
                    source_lot_code as source_lot_code,
                    lot_guid,
                    lot_status_code,
                    case
                        when ukp_normalized_item.normalized_value is null
                        then lot_status_code
                        else ukp_normalized_item.normalized_value
                    end as lot_status_desc,
                    source_business_unit_code,
                    plantdc_address_guid as business_unit_address_guid,
                    transaction_amt,
                    nvl(reason_code,'-' ) as reason_code,
                    transaction_date,
                    nvl(remark_txt,'-') as remark_txt,
                    transaction_qty,
                    case
                        when ukp_normalized_primary.normalized_value is null
                        then transaction_uom
                        else ukp_normalized_primary.normalized_value
                    end as v_transaction_uom,
                    v_transaction_uom as transaction_uom,
                    transaction_unit_cost,
                    nvl(transaction_currency,'-') as transaction_currency, 

                    case
                        when v_transaction_uom='KG' AND PRIMARY_UOM='LB' then 2.20462
                        when v_transaction_uom='LB' AND PRIMARY_UOM='KG' then 0.453592
                        when v_transaction_uom is null or PRIMARY_UOM = v_transaction_uom then 1
                        when
                            primary_uom = 'KG'
                            and uom_factor_lb.conversion_rate is not null
                        then uom_factor_lb.conversion_rate * {{ent_dbt_package.lkp_constants("LB_KG_CONVERSION_RATE")}}--0.453592
                        when uom_factor_primary.conversion_rate is not null
                        then uom_factor_primary.conversion_rate
                        else 0
                    end as v_conversion_rate,
				
				    transaction_qty * v_conversion_rate as v_transaction_pri_uom_qty,
                    v_transaction_pri_uom_qty as transaction_pri_uom_qty,
                    case when v_conversion_rate is null or v_conversion_rate = 0 then 0 else transaction_unit_cost/v_conversion_rate end as v_transaction_pri_uom_unit_cost, 
                    v_transaction_pri_uom_unit_cost as transaction_pri_uom_unit_cost,
					
                    (
                       case when ( v_transaction_pri_uom_unit_cost::number(27, 9) * v_transaction_pri_uom_qty::number(27, 9))=0 and  src.TRANSACTION_AMT <> 0 
					   then src.TRANSACTION_AMT
					   else v_transaction_pri_uom_unit_cost::number(27, 9) * v_transaction_pri_uom_qty::number(27, 9) end
                    ) as transaction_pri_uom_amt,
				     SRC.source_system,
                    src.SOURCE_DOCUMENT_TYPE as source_document_type,
                    src.source_original_document_type as source_original_document_type,
                    case
                        when ukp_normalized_reason.normalized_value is null
                        then reason_code
                        else ukp_normalized_reason.normalized_value
                    end as reason_code_desc,
                    systimestamp() as load_date,
                    systimestamp() as update_date,
                    uom_factor_kg_uom.conversion_rate as v_pri_to_kg_conv,

                    uom_factor_lb_uom.conversion_rate as v_pri_to_lb_conv,

                    uom_factor_cw_uom.conversion_rate as v_pri_to_cw_conv,

                    case
                        when primary_uom = 'KG'
                        then 1
                        when primary_uom = 'LB'
                        then 0.453592
                        when primary_uom = 'CW'
                        then 45.3592
                        when v_pri_to_kg_conv is not null
                        then v_pri_to_kg_conv
                        when v_pri_to_lb_conv is not null
                        then v_pri_to_lb_conv * 0.453592
                        when v_pri_to_cw_conv is not null
                        then v_pri_to_lb_conv * 45.3592
                        else 0
                    end as v_transaction_kg_conv,

                    v_transaction_kg_conv
                    * v_transaction_pri_uom_qty as transaction_kg_qty,
                    v_transaction_kg_conv
                    * v_transaction_pri_uom_qty
                    * 2.20462 as transaction_lb_qty,
                  
					pallet_count,
			        src.TRANSACTION_AMT*BASE_CONV_RT_LKP.CURR_CONV_RT as BASE_AMT ,
                    src.BASE_CURRENCY,
                    unique_key,
                    src.source_pallet_id,
                    src.VARIANT

                from
                    (
                        select
                            stg.source_system as source_system,
							stg.source_document_type as source_document_type,
							stg.source_original_document_type as source_original_document_type,
							stg.related_address_number as related_address_number,
							stg.gl_date as gl_date,
							stg.document_number as document_number,
							nvl(stg.original_document_number,'-') as original_document_number,
							stg.source_item_identifier as source_item_identifier,
							stg.document_company as document_company,
							/*stg.original_document_company as original_document_company,*/
							stg.line_number as line_number,
							stg.original_line_number as original_line_number,
							upper(stg.source_location_code)as source_location_code,
							upper(stg.source_lot_code) as source_lot_code,
							upper(stg.lot_status_code) as lot_status_code,
							upper(trim(stg.source_business_unit_code)) as source_business_unit_code,
							stg.transaction_amt as transaction_amt,
							stg.reason_code as reason_code,
							stg.transaction_date as transaction_date,
							stg.remark_txt as remark_txt,
							stg.transaction_qty as transaction_qty,
							stg.transaction_uom as transaction_uom,
							stg.transaction_unit_cost as transaction_unit_cost,
							/*stg.transaction_currency as transaction_currency,*/
							stg.source_update_date as source_update_date,
							stg.source_pallet_id as source_pallet_id,
							stg.variant as variant,
							stg.pallet_count as pallet_count,
							ITM.ITEM_GUID AS ITEM_GUID,
							ITM.PRIMARY_UOM AS PRIMARY_UOM,
							adr.address_guid as address_guid,
							/*com.company_code as document_company,*/
							com.default_currency_code as transaction_currency,
							nvl(com_org.company_code,'-') as original_document_company,
							{{
                                dbt_utils.surrogate_key(
                                    [
                                        "stg.SOURCE_SYSTEM",
                                        "upper(stg.SOURCE_LOCATION_CODE)",
                                        "upper(trim(stg.source_business_unit_code))",
                                    ]
                                )
                            }} as location_guid,
							 {{
                                dbt_utils.surrogate_key(
                                    [
                                        "stg.SOURCE_SYSTEM",
                                        "upper(trim(stg.source_business_unit_code))",
                                        "stg.SOURCE_ITEM_IDENTIFIER",
                                        "upper(stg.source_lot_code)",
                                    ]
                                )
                            }} as lot_guid,
                            {{
                                dbt_utils.surrogate_key(
                                    [
                                        "stg.SOURCE_SYSTEM",
                                        "upper(trim(stg.source_business_unit_code))",
                                        "'PLANT_DC'",
                                    ]
                                )
                            }} as plantdc_address_guid,
                            eff_curr_dim.COMPANY_DEFAULT_CURRENCY_CODE as base_currency,
                             {{
                                dbt_utils.surrogate_key(
                                    [
                                        "stg.DOCUMENT_NUMBER",
                                        "stg.document_company",
                                        "stg.LINE_NUMBER",
                                        "stg.TRANSACTION_DATE",
                                        "stg.SOURCE_SYSTEM",
                                        "stg.SOURCE_DOCUMENT_TYPE"
                                    ]
                                )
                            }} as unique_key
                        from source_details stg
                        left join
                      dim_wbx_address adr
                            on adr.address_guid = stg.CUSTOMER_ADDRESS_NUMBER_GUID
                        left join  /* EI_RDM.ADR_COMPANY_MASTER_DIM */
                            dim_wbx_company com
                            on com.source_system = stg.source_system
                            and com.company_code = stg.document_company
						left join  /* EI_RDM.ADR_COMPANY_MASTER_DIM */
                            dim_wbx_company com_org
                            on com_org.source_system = stg.source_system
                            and com_org.company_code = stg.ORIGINAL_DOCUMENT_COMPANY
						LEFT JOIN dim_wbx_item ITM
							ON ITM.SOURCE_SYSTEM = STG.SOURCE_SYSTEM
							AND ITM.SOURCE_ITEM_IDENTIFIER = STG.SOURCE_ITEM_IDENTIFIER
							AND ITM.SOURCE_BUSINESS_UNIT_CODE = upper(trim(stg.source_business_unit_code))
						left join ref_effective_currency_dim eff_curr_dim 
							on eff_curr_dim.source_system = stg.source_system
							and eff_curr_dim.source_business_unit_code = upper(trim(stg.source_business_unit_code))
							and eff_curr_dim.effective_date <= stg.gl_date
							and eff_curr_dim.expiration_date >= stg.gl_date
                     
                    ) SRC
				left join
                    {{
                        lkp_normalization(
                            'TRIM(UPPER(SRC.SOURCE_SYSTEM))',
                            'FINANCE',
                            'INV_DOC_TYPE_CODE',
                            'TRIM(UPPER(SRC.SOURCE_DOCUMENT_TYPE))',
                            'UKP_NORMALIZED_DOC',
                        )
                    }}
				left join
                    {{
                        lkp_normalization(
                            'TRIM(UPPER(SRC.SOURCE_SYSTEM))',
                            'FINANCE',
                            'INV_DOC_TYPE_CODE',
                            'TRIM(UPPER(SRC.SOURCE_ORIGINAL_DOCUMENT_TYPE))',
                            'UKP_NORMALIZED_ORG_DOC',
                        )
                    }}	
				left join	
				 {{
                        lkp_normalization(
                            'TRIM(UPPER(SRC.SOURCE_SYSTEM))',
                            'ITEM',
                            'LOT_STATUS_DESC',
                            'TRIM(UPPER(SRC.LOT_STATUS_CODE))',
                            'UKP_NORMALIZED_ITEM',
                        )
                    }}	
				
				left join
                    {{
                        lkp_normalization(
                            'TRIM(UPPER(SRC.SOURCE_SYSTEM))',
                            'LOGISTICS',
                            'REASON_CODE_DESC',
                            'TRIM(UPPER(SRC.REASON_CODE))',
                            'UKP_NORMALIZED_REASON',
                        )
                    }}
				left join
                    {{
                        lkp_normalization(
                            'TRIM(UPPER(SRC.SOURCE_SYSTEM))',
                            'ITEM',
                            'PRIMARY_UOM',
                            'TRIM(UPPER(SRC.TRANSACTION_UOM))',
                            'UKP_NORMALIZED_PRIMARY',
                        )
                    }}
					
					 left join
                    {{
                        ent_dbt_package.lkp_uom(
                            "SRC.ITEM_GUID",
                            "SRC.TRANSACTION_UOM",  "'LB'",                          
                            "UOM_FACTOR_LB",
                        )
                    }}
					
					 left join
                    {{
                        ent_dbt_package.lkp_uom(
                            "SRC.ITEM_GUID",
                            "SRC.TRANSACTION_UOM", "SRC.PRIMARY_UOM",                           
                            "UOM_FACTOR_PRIMARY",
                        )
                    }}
					left join
                    {{
                        ent_dbt_package.lkp_uom(
                            "SRC.ITEM_GUID",
                            "SRC.PRIMARY_UOM",
                            "'KG'",
                            "UOM_FACTOR_KG_UOM",
                        )
                    }}
                left join
                    {{
                        ent_dbt_package.lkp_uom(
                            "SRC.ITEM_GUID",
                            "SRC.PRIMARY_UOM",
                            "'LB'",
                            "UOM_FACTOR_LB_UOM",
                        )
                    }}
                left join
                    {{
                        ent_dbt_package.lkp_uom(
                            "SRC.ITEM_GUID", "SRC.PRIMARY_UOM", "'CW'", "UOM_FACTOR_CW_UOM"
                        )
                    }}
                 LEFT JOIN {{ ent_dbt_package.lkp_exchange_rate_daily("src.TRANSACTION_CURRENCY","src.BASE_CURRENCY",'SRC.TRANSACTION_DATE','BASE_CONV_RT_LKP')  }}  
             
               where src.item_guid is not null

                
               
            )
        
    ),


    target as (select 
    
    cast(substring(related_address_number,1,255) as text(255) ) as related_address_number  ,

    cast(address_guid as text(255) ) as address_guid  ,

    cast(substring(document_type,1,20) as text(20) ) as document_type  ,

    cast(substring(original_document_type,1,20) as text(20) ) as original_document_type  ,

    cast(gl_date as timestamp_ntz(9) ) as gl_date  ,

    cast(substring(document_number,1,255) as text(255) ) as document_number  ,

    cast(substring(original_document_number,1,255) as text(255) ) as original_document_number  ,

    cast(substring(source_item_identifier,1,255) as text(255) ) as source_item_identifier  ,

    cast(item_guid as text(255) ) as item_guid  ,

    cast(substring(document_company,1,20) as text(20) ) as document_company  ,

    cast(substring(original_document_company,1,20) as text(20) ) as original_document_company  ,

    cast(original_line_number as number(38,10) ) as original_line_number  ,

    cast(line_number as number(38,10) ) as line_number  ,

    cast(substring(source_location_code,1,255) as text(255) ) as source_location_code  ,

    cast(location_guid as text(255) ) as location_guid  ,

    cast(substring(source_lot_code,1,255) as text(255) ) as source_lot_code  ,

    cast(lot_guid as text(255) ) as lot_guid  ,

    cast(substring(lot_status_code,1,255) as text(255) ) as lot_status_code  ,

    cast(substring(lot_status_desc,1,30) as text(30) ) as lot_status_desc  ,

    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(business_unit_address_guid as text(255) ) as business_unit_address_guid  ,

    cast(transaction_amt as number(27,9) ) as transaction_amt  ,

    cast(substring(reason_code,1,255) as text(255) ) as reason_code  ,

    cast(transaction_date as timestamp_ntz(9) ) as transaction_date  ,

    cast(substring(remark_txt,1,255) as text(255) ) as remark_txt  ,

    cast(transaction_qty as number(27,9) ) as transaction_qty  ,

    cast(substring(transaction_uom,1,20) as text(20) ) as transaction_uom  ,

    cast(transaction_unit_cost as number(27,9) ) as transaction_unit_cost  ,

    cast(substring(transaction_currency,1,20) as text(20) ) as transaction_currency  ,

    cast(transaction_pri_uom_qty as number(27,9) ) as transaction_pri_uom_qty  ,

    cast(transaction_pri_uom_unit_cost as number(27,9) ) as transaction_pri_uom_unit_cost  ,

    cast(transaction_pri_uom_amt as number(27,9) ) as transaction_pri_uom_amt  ,

    cast(substring(base_currency,1,20) as text(20) ) as base_currency  ,

    cast(base_amt as number(27,9) ) as base_amt  ,

    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(source_document_type,1,20) as text(20) ) as source_document_type  ,

    cast(substring(source_original_document_type,1,20) as text(20) ) as source_original_document_type  ,

    cast(substring(reason_code_desc,1,255) as text(255) ) as reason_code_desc  ,

    cast(load_date as timestamp_ntz(9) ) as load_date  ,

    cast(update_date as timestamp_ntz(9) ) as update_date  ,

    cast(transaction_kg_qty as number(27,9) ) as transaction_kg_qty  ,

    cast(transaction_lb_qty as number(27,9) ) as transaction_lb_qty  ,

    cast(substring(source_pallet_id,1,255) as text(255) ) as source_pallet_id  ,

    cast(substring(variant,1,255) as text(255) ) as variant  ,

    cast(pallet_count as number(15,2) ) as pallet_count  ,
 
    cast(unique_key as text(255) ) as unique_key
	from joined_source)

select *
from target