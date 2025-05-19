-- Viết các tests tùy chỉnh cho các trường hợp phức tạp
WITH data AS (
    SELECT *
    FROM {{ ref('my_model') }}
)
SELECT *
FROM data
WHERE id IS NULL;
