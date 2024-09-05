/*createing this view(joins ecoresproduct and ecoresproducttranslation ) to overcome issue with stg_d_wbx_mfg_wo_itm_bom long running issue*/

{{
    config(
        materialized=env_var("DBT_MAT_VIEW")
    )
}}
with ecoresproduct as (select * from {{ ref("src_ecoresproduct") }}),
     ecoresproducttranslation as (select * from {{ ref("src_ecoresproducttranslation") }} ),

     final as(
        select 
        erpmaterial.recid as erp_recid,
        erpmaterial.partition as erp_partition,
        erptmaterial.product as erpt_product,
        erptmaterial.partition as erpt_partition
        from 
        ecoresproduct erpmaterial
        inner join
        ecoresproducttranslation erptmaterial
        on erptmaterial.product = erpmaterial.recid
        )


select * from final