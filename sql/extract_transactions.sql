-- Incremental extraction for transactions table
SELECT
    transaction_id,
    customer_id,
    amount,
    currency,
    transaction_date,
    status,
    updated_at
FROM transactions
WHERE transaction_date > :watermark
ORDER BY transaction_date;