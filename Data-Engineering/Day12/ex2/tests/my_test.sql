-- Test kiểm tra dữ liệu có hợp lệ hay không
SELECT
    COUNT(*) = 0 AS all_data_valid
FROM {{ ref('my_model') }}
WHERE name NOT IN ('ValidName1', 'ValidName2', 'ValidName3');
