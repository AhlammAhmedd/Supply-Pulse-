

with source as (

    select * from {{ source('catalog_pim', 'products') }}

),

typed as (

    select
        product_id,
        barcode,
        {{ is_valid_barcode('barcode') }}        as is_valid_barcode,   
        {{ fix_upper_case(clean_null('product_name')) }} as product_name,
        {{ clean_null('brand') }}                as brand,
        trim(category)                           as category,           
        {{ normalize_pack_size('pack_size') }}   as pack_size,
        {{ to_number_clean('base_price_egp') }}  as base_price_egp,
        {{ to_boolean_clean('is_perishable') }}  as is_perishable,
        {{ to_boolean_clean('is_active') }}      as is_active

    from source

)

select * from typed