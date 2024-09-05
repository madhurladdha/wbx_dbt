{{ config(enabled=false, severity="warn") }}


with
    a as (

        select

            "SOURCE_SYSTEM",
            "SOURCE_ITEM_IDENTIFIER",
            "SOURCE_BUSINESS_UNIT_CODE",
            "CALENDAR_DATE",
            "WORK_CENTER_CODE",
            "WORK_CENTER_DESC",
            "SCHEDULED_QTY",
            "CANCELLED_QTY",
            "PRODUCED_QTY",
            "SCHEDULED_KG_QTY",
            round("PRODUCED_LB_QTY", 3),
            "ORIG_SCHEDULED_QTY",
            "ORIG_SCHEDULED_KG_QTY",
            "PRODUCED_KG_QTY",
            "CTP_TARGET_PERCENT",
            "PTP_TARGET_PERCENT",
            "UNIQUE_KEY"

        from {{ref('conv_fct_wbx_mfg_wo_item_agg')}}

    ),

    b as (

        select

            "SOURCE_SYSTEM",
            "SOURCE_ITEM_IDENTIFIER",
            "SOURCE_BUSINESS_UNIT_CODE",
            "CALENDAR_DATE",
            "WORK_CENTER_CODE",
            "WORK_CENTER_DESC",
            "SCHEDULED_QTY",
            "CANCELLED_QTY",
            "PRODUCED_QTY",
            "SCHEDULED_KG_QTY",
            round("PRODUCED_LB_QTY", 3),
            "ORIG_SCHEDULED_QTY",
            "ORIG_SCHEDULED_KG_QTY",
            "PRODUCED_KG_QTY",
            "CTP_TARGET_PERCENT",
            "PTP_TARGET_PERCENT",
            "UNIQUE_KEY"

        from {{ref('fct_wbx_mfg_wo_item_agg')}}

    ),

    a_intersect_b as (

        select *
        from a

        intersect

        select *
        from b

    ),

    a_except_b as (

        select *
        from a

        except

        select *
        from b

    ),

    b_except_a as (

        select *
        from b

        except

        select *
        from a

    ),

    all_records as (

        select *, true as in_a, true as in_b
        from a_intersect_b

        union all

        select *, true as in_a, false as in_b
        from a_except_b

        union all

        select *, false as in_a, true as in_b
        from b_except_a

    ),

    final as (

        select *
        from all_records
        where not (in_a and in_b)
        order by unique_key, in_a desc, in_b desc

    )

select *
from final
