import json
import random

import redis


r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)


for i in range(10000, 99999):

    customer_profile = {

        "customer_risk_score": round(
            random.uniform(0.01, 0.99),
            4
        ),

        "kyc_verified": random.choice(
            [True, True, True, False]
        ),

        "account_age_days": random.randint(
            30,
            4000
        ),

        "previous_fraud_count": random.randint(
            0,
            5
        ),

        "avg_transaction_amount": round(
            random.uniform(50, 5000),
            2
        )
    }

    r.set(
        f"CUST_{i}",
        json.dumps(customer_profile)
    )

print("Redis customer profiles loaded successfully.")