TRANSACTION_SCHEMA = {
    "transaction_id": "string",
    "event_time": "string",          # ISO-8601 UTC
    "customer_id": "string",
    "account_id": "string",
    "card_id": "string",
    "merchant_id": "string",
    "merchant_category": "string",
    "transaction_type": "string",    # card_present | card_not_present | atm | transfer
    "amount": "float",
    "currency": "string",
    "country": "string",
    "city": "string",
    "device_id": "string",
    "ip_address": "string",
    "channel": "string",             # mobile | web | pos | atm
    "is_international": "bool",
    "is_velocity_burst": "bool",
    "is_high_risk_merchant": "bool",
    "baseline_risk_score": "float",
    "fraud_label": "int"             # 0 or 1 for training/simulation
}