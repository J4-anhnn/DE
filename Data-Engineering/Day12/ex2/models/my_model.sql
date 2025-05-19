-- Câu lệnh SQL để tạo mô hình dữ liệu trong dbt
WITH raw_data AS (
    SELECT *
    FROM {{ ref('my_seed') }}
)
SELECT
    id,
    name,
    created_at,
    updated_at
FROM raw_data
WHERE created_at IS NOT NULL;
