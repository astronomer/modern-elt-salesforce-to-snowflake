INSERT INTO customers
SELECT
    name,
    'Y' as is_active,
    CURRENT_TIMESTAMP() AS create_datetime,
    CURRENT_TIMESTAMP() AS last_update_datetime
FROM customers_staging stg
WHERE name != '[not provided]'
  AND NOT EXISTS (
    SELECT 1
    FROM customers
    WHERE stg.name = customers.name
);
