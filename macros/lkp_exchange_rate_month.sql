{%- macro lkp_exchange_rate_month(FROM_CURRENCY_CODE ,TO_CURRENCY_CODE,FISCAL_YEAR_PERIOD_NO,EXCH_RATE_TYPE,ALIAS) -%}
(SELECT FROM_CURRENCY_CODE ,TO_CURRENCY_CODE,FISCAL_YEAR_PERIOD_NO,EXCH_RATE_TYPE,CURR_CONVERSION_RT,
ROW_NUMBER() over (partition by FROM_CURRENCY_CODE ,TO_CURRENCY_CODE,FISCAL_YEAR_PERIOD_NO,EXCH_RATE_TYPE order by FROM_CURRENCY_CODE ,TO_CURRENCY_CODE,FISCAL_YEAR_PERIOD_NO,EXCH_RATE_TYPE ) AS ROW_NUM
    FROM ( {{ ref('v_dim_exchange_rate_mth')}} ) ) {{ALIAS}}
    ON {{ALIAS}}.FROM_CURRENCY_CODE = {{ FROM_CURRENCY_CODE }}
    AND {{ALIAS}}.TO_CURRENCY_CODE = {{ TO_CURRENCY_CODE }}
    AND {{ALIAS}}.FISCAL_YEAR_PERIOD_NO = CASE WHEN {{ FROM_CURRENCY_CODE }}={{ TO_CURRENCY_CODE }} THEN '190001' ELSE {{ FISCAL_YEAR_PERIOD_NO }} END
    AND {{ALIAS}}.EXCH_RATE_TYPE = CASE WHEN {{ FROM_CURRENCY_CODE }}={{ TO_CURRENCY_CODE }} THEN 'dummy' ELSE {{ EXCH_RATE_TYPE }} END
    AND {{ALIAS}}.ROW_NUM =1
{%- endmacro -%}