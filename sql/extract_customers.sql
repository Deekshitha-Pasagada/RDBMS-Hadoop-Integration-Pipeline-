-- Full extraction query for customers table
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    age,
    country,
    account_balance,
    signup_date,
    updated_at
FROM customers
WHERE updated_at > :watermark
ORDER BY updated_at;