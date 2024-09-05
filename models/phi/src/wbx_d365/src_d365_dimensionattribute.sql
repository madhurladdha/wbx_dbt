
with

d365_source as (
    select *
    from {{ source("D365", "dimension_attribute") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (

    select
        'D365' as source,
        name as name,
        key_attribute as keyattribute,
        value_attribute as valueattribute,
        name_attribute as nameattribute,
        backing_entity_type as backingentitytype,
        report_column_name as reportcolumnname,
        hash_key as hashkey,
        type as type,
        view_name as viewname,
        backing_entity_table_name as backingentitytablename,
        backing_entity_key_field_name as backingentitykeyfieldname,
        backing_entity_value_field_name as backingentityvaluefieldname,
        backing_entity_table_id as backingentitytableid,
        backing_entity_key_field_id as backingentitykeyfieldid,
        backing_entity_value_field_id as backingentityvaluefieldid,
        is_balancing_psn as isbalancing_psn,
        balancing_dimension_psn as balancingdimension_psn,
        null as docudatasourcequeryname,
        translation_key_field_id as translationkeyfieldid,
        translation_key_field_name as translationkeyfieldname,
        translation_language_id_field_id as translationlanguageidfieldid,
        translation_language_id_field_name as translationlanguageidfieldname,
        translation_name_field_id as translationnamefieldid,
        translation_name_field_name as translationnamefieldname,
        translation_table_id as translationtableid,
        translation_table_name as translationtablename,
        translation_view_id as translationviewid,
        translation_view_key_field_id as translationviewkeyfieldid,
        translation_view_key_field_name as translationviewkeyfieldname,
        translation_view_language_id_field_id as translationviewlangidfieldid,
        translation_view_language_id_field_name
            as translationviewlangidfieldname,
        translation_view_name as translationviewname,
        translation_view_name_field_id as translationviewnamefieldid,
        translation_view_name_field_name as translationviewnamefieldname,
        translation_view_system_language_id_field_id
            as transviewsystemlangidfieldid,
        translation_view_system_language_id_field_name
            as transviewsystemlangidfieldname,
        translation_view_translated_name_field_id
            as transviewtranslatednamefieldid,
        translation_view_translated_name_field_name
            as transviewtransnamefieldname,
        translation_view_value_field_id as translationviewvaluefieldid,
        translation_view_value_field_name as translationviewvaluefieldname,
        use_translation_name_method as usetranslationnamemethod,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed