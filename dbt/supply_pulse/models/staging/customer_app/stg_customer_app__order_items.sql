
with source as (
    select * from {{ source('customer_app', 'order_items') }}
),

--line number + order id = grain. remove duplicates
deduplicated as (
    select * from source
    qualify row_number() over (
        partition by order_id, line_number
        order by order_id
    ) = 1
),

-- Convert every raw text column to its proper type
typed as (
    select
        order_id,
        line_number,

        -- Unify id formats: p00012 / P00012 -> SKU-00012, m0000018 -> M0000018
        {{ normalize_item_ref('item_ref') }}              as item_ref,

        -- Empty/'null'/'N/A' -> NULL first, then fix ALL-UPPER names
        {{ fix_upper_case(clean_null('item_name')) }}     as item_name,

        -- "1 pcs" -> 1. 
   
        try_to_number(regexp_substr(trim(quantity), '^-?[0-9]+')) as quantity,

   
        {{ to_number_clean('unit_price') }}               as unit_price,

        
        {{ to_number_clean('line_total') }}               as line_total_raw,

        {{ to_number_clean('vendor_confirmed_price') }}   as vendor_confirmed_price
    from deduplicated
),

-- Business rules that need already-typed columns
cleaned as (
    select
        order_id,
        line_number,
        item_ref,
        item_name,
        quantity,

        -- Zero or negative quantity is illogical.
     
        quantity <= 0 as is_invalid_quantity,

        unit_price,

        -- Expected: line_total ~ quantity * unit price (ratio ~ 1).
        -- If the ratio is ~ 100 the value is in piasters, so divide by 100.
        -- coalesce is used only for this comparison, not to fill unit_price.
        -- quantity > 0 is required, otherwise the ratio is meaningless.
        case
            when quantity > 0
             and div0null(
                    line_total_raw,
                    quantity * coalesce(unit_price, vendor_confirmed_price)
                 ) between 90 and 110
            then round(line_total_raw / 100, 2)
            else line_total_raw
        end                                               as line_total,

        vendor_confirmed_price
    from typed
),

-- Decision 1: rebuild a missing unit_price as line_total / quantity
-- (using the corrected line_total). Otherwise it stays NULL.
-- vendor_confirmed_price is never used to fill unit_price.
priced as (
    select
        order_id,
        line_number,
        item_ref,
        item_name,
        quantity,
        is_invalid_quantity,

        coalesce(
            unit_price,
            case
                when quantity > 0 and line_total is not null
                then round(line_total / quantity, 2)
            end
        )   as unit_price,

        -- Traceability: true only where we rebuilt the price
        (unit_price is null
         and quantity > 0
         and line_total is not null) as is_unit_price_rebuilt,

        line_total,
        vendor_confirmed_price
    from cleaned
)

select * from priced