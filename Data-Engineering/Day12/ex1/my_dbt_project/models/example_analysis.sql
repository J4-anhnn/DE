SELECT 
    order_id,
    {{ cents_to_dollars('total_amount') }} as amount_in_dollars
FROM {{ source('raw_data', 'orders') }}
WHERE {{ cents_to_dollars('total_amount') }} > 10
LIMIT 10