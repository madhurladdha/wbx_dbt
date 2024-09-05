

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_ROB_ImpactOption') }}

),

renamed as (

    select
        impactoption_idx,
        impactoption_code,
        impactoption_name,
        impact_idx,
        impact_format,
        apbuildwave,
        islumpsumtype,
        isvolumetype,
        isratetype,
        ispercenttype,
        allowoverlaps,
        showingroupcreator,
        showincontracts,
        showinsingleeditor,
        requiresqfxspreading

    from source

)

select * from renamed
