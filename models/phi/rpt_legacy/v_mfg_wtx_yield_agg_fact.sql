{{ config(materialized=env_var("DBT_MAT_VIEW"), tags=["weetabix","yield","aggregate","agg","wbx","fact","mfg","manufacturing"]) }}

with
cte_mfg_wtx_yield_agg_fact as (select * from {{ ref('fct_wbx_mfg_yield_agg') }}),
cte_mfg_wtx_wo_produced_fact as (select * from {{ ref('fct_wbx_mfg_wo_produced') }}),
cte_dim_date as (select * from {{ ref('src_dim_date') }}),
cte_itm_item_master_dim as (select * from {{ ref('dim_wbx_item') }}),

final_v_mfg_wtx_yield_agg_fact as 
(
    SELECT 
        Y.SOURCE_SYSTEM,
        Y.COMP_STOCK_SITE,
        Y.FINANCIAL_SITE,
        Y.VOUCHER,
        Y.WORK_ORDER_NUMBER,
        Y.COMP_SRC_ITEM_IDENTIFIER,
        Y.COMP_SRC_VARIANT_CODE,
        Y.TRANSACTION_DATE,
        Y.COMP_ITEM_TYPE,
        Y.SOURCE_BOM_IDENTIFIER,
        Y.WO_SRC_ITEM_IDENTIFIER,
        Y.WO_SRC_VARIANT_CODE,
        Y.SOURCE_BUSINESS_UNIT_CODE,
        Y.COMPANY_CODE,
        Y.COMP_TRANSACTION_UOM,
        Y.TRANSACTION_CURRENCY,
        Y.ACTUAL_TRANSACTION_QTY,
        Y.COMP_STANDARD_QUANTITY,
        Y.COMP_PERFECTION_QUANTITY,
        Y.COMP_SCRAP_PERCENT,
        Y.ITEM_MATCH_BOM_FLAG,
        Y.TRANSACTION_AMT,
        Y.STOCK_ADJ_QTY,
        Y.PRODUCT_CLASS,
        Y.CONSOLIDATED_BATCH_ORDER,
        Y.BULK_FLAG,
        Y.TRANDT_ACTUAL_AMOUNT,
        Y.GLDT_ACTUAL_AMOUNT,
        Y.STANDARD_AMOUNT,
        Y.PERFECTION_AMOUNT,
        Y.GLDT_STOCK_ADJ_AMOUNT,
        Y.LOAD_DATE,
        Y.UPDATE_DATE,
        Y.COMP_ITEM_MODEL_GROUP,
        Y.WO_ITEM_MODEL_GROUP,
        Y.WO_STOCK_SITE,
        Y.FLAG,
        D.FISCAL_PERIOD_NO,
        D.FISCAL_PERIOD_BEGIN_DT,
        D.FISCAL_PERIOD_END_DT,
        D.FISCAL_YEAR_BEGIN_DT,
        D.FISCAL_YEAR_END_DT,
        D.REPORT_FISCAL_YEAR_PERIOD_NO,
        D.REPORT_FISCAL_YEAR,
        P.PRODUCED_QTY,
        I.DESCRIPTION AS COMP_DESCRIPTION,
        WO.DESCRIPTION AS WO_DESCRIPTION
    FROM cte_mfg_wtx_yield_agg_fact Y
        LEFT JOIN cte_dim_date D
            ON Y.TRANSACTION_DATE = D.CALENDAR_DATE
        LEFT JOIN cte_mfg_wtx_wo_produced_fact P
            ON Y.BULK_FLAG = P.BULK_FLAG
            AND Y.COMPANY_CODE = P.COMPANY_CODE
            AND Y.CONSOLIDATED_BATCH_ORDER = P.CONSOLIDATED_BATCH_ORDER
            AND Y.WO_ITEM_MODEL_GROUP = P.ITEM_MODEL_GROUP
            AND Y.PRODUCT_CLASS = P.PRODUCT_CLASS
            AND Y.SOURCE_BOM_IDENTIFIER = P.SOURCE_BOM_IDENTIFIER
            AND Y.SOURCE_SYSTEM = P.SOURCE_SYSTEM
            AND Y.VOUCHER = P.VOUCHER
            AND Y.WORK_ORDER_NUMBER = P.WORK_ORDER_NUMBER
        LEFT JOIN (SELECT DISTINCT SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER, MAX(DESCRIPTION) AS DESCRIPTION FROM cte_itm_item_master_dim WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' GROUP BY SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER) I
            ON Y.COMP_SRC_ITEM_IDENTIFIER = I.SOURCE_ITEM_IDENTIFIER
            AND Y.SOURCE_SYSTEM = I.SOURCE_SYSTEM
        LEFT JOIN (SELECT DISTINCT SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER, MAX(DESCRIPTION) AS DESCRIPTION FROM cte_itm_item_master_dim WHERE SOURCE_SYSTEM = '{{env_var("DBT_SOURCE_SYSTEM")}}' GROUP BY SOURCE_SYSTEM, SOURCE_ITEM_IDENTIFIER) WO
            ON Y.WO_SRC_ITEM_IDENTIFIER = WO.SOURCE_ITEM_IDENTIFIER
            AND Y.SOURCE_SYSTEM = WO.SOURCE_SYSTEM

    )
    select * from final_v_mfg_wtx_yield_agg_fact
