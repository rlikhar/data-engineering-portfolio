WITH base AS (

    SELECT *

    FROM {{ ref('stg_silver_transactions') }}

)

SELECT

    bronze_date,

    customer_id,

    COUNT(*) AS total_transactions,

    SUM(
        CASE
            WHEN fraud_rule_triggered = TRUE
            THEN 1
            ELSE 0
        END
    ) AS fraud_transactions,

    ROUND(AVG(amount), 2) AS avg_transaction_amount,

    ROUND(SUM(amount), 2) AS total_transaction_amount,

    ROUND(AVG(customer_risk_score), 4) AS avg_customer_risk_score,

    MAX(customer_risk_score) AS max_customer_risk_score

FROM base

GROUP BY 1, 2