

with source as (

    select * from {{ source('WEETABIX', 'MDS_TBLDIMCOMMODITYCHG') }}

),

renamed as (

select
        id,
        muid,
        versionname,
        versionnumber,
        versionflag,
        name,
        code,
        changetrackingmask,
        moddate,
        startdate,
        kmetaaudit,
        category,
        category_desc,
        subcategory,
        subcategory_desc,
        productclass,
        productclass_desc,
        power_brand_code,
        power_brand_desc,
        productsubproduct,
        productsubproduct_desc,
        manufacturing_group_code,
        manufacturing_group_desc,
        mangrpcd_site,
        mangrpcd_plant,
        mangrpcd_copack_flag,
        productsource,
        productexternalclass,
        productexternalclass_desc,
        productpromotiontype,
        productpromotiontype_desc,
        productbranding,
        productbranding_desc,
        productpacksize,
        productpacksize_desc,
        kcountry,
        dc_fgitemdesc,
        dc_equivalentfactor,
        dc_multdivindicator,
        dc_inactive,
        dc_statsonly,
        dc_caseweight,
        dc_cusweight,
        dc_stdweight,
        dc_pdcweight,
        dc_equivcaseweight,
        dc_palletqty,
        dc_caseslayer,
        dc_halfpalletqty,
        dc_palletindicator,
        dc_shelflife,
        dc_packsize,
        dc_excludeindicator,
        dc_outdepth,
        dc_outlength,
        dc_outwidth,
        dc_packscase,
        dc_packdepth,
        dc_packlength,
        dc_packwidth,
        dc_anacde,
        dc_anamdlcde,
        dc_anapltcde,
        dc_anatrucde,
        dc_tariffcde,
        dc_nomcde,
        dc_vatcde,
        dc_insdatetime,
        dc_lastamendeddatetime,
        dc_noofbars,
        krecipe,
        kproductdimension,
        dc_datenpdstart,
        dc_datenpdfinish,
        enterdatetime

from source

)

select * from renamed
