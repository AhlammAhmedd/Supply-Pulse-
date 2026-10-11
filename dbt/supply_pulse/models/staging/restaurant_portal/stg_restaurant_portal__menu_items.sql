

with source as (

    select * from {{ source('restaurant_portal', 'menu_items') }}

),


deduplicated as (

    select * from source
    qualify row_number() over (partition by menu_item_id order by menu_item_id) = 1

),


typed as (

    select
        menu_item_id,
        vendor_id,
        {{ clean_null("rtrim(item_name, ' .')") }}  as item_name,
        trim(menu_section)                           as raw_section,
        {{ to_number_clean('price_egp') }}           as price_raw,
        {{ to_boolean_clean('is_vegetarian') }}      as is_vegetarian
    from deduplicated

),


cleaned as (

    select
        menu_item_id,
        vendor_id,
        {{ fix_upper_case('item_name') }}            as item_name,
        raw_section                                  as menu_section,   
        {{ piasters_to_egp('price_raw') }}           as price_egp,
        is_vegetarian

    from typed

)

select * from cleaned