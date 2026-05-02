from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

from faker import Faker

fake = Faker()


@dataclass
class GeneratorConfig:
    fraud_rate: float = 0.03
    high_risk_merchant_rate: float = 0.12
    international_rate: float = 0.18
    velocity_burst_rate: float = 0.08


TRANSACTION_TYPES = ["card_present", "card_not_present", "atm", "transfer"]
CHANNELS = ["mobile", "web", "pos", "atm"]
CURRENCIES = ["USD", "INR", "EUR", "GBP"]
HIGH_RISK_MERCHANTS = [
    "gaming",
    "crypto_exchange",
    "gift_cards",
    "electronics",
    "travel",
    "wire_transfer",
]
MERCHANT_CATEGORIES = [
    "groceries",
    "fuel",
    "food_delivery",
    "fashion",
    "electronics",
    "gaming",
    "travel",
    "crypto_exchange",
    "gift_cards",
    "utilities",
]


class TransactionGenerator:
    def __init__(self, config: GeneratorConfig | None = None):
        self.config = config or GeneratorConfig()

    def _risk_score(self, amount: float, international: bool, high_risk: bool, velocity_burst: bool) -> float:
        score = 0.08
        score += min(amount / 50000.0, 0.30)
        score += 0.20 if international else 0.0
        score += 0.25 if high_risk else 0.0
        score += 0.30 if velocity_burst else 0.0
        return round(min(score, 0.99), 4)

    def generate_transaction(self) -> Dict:
        fraud_label = 1 if random.random() < self.config.fraud_rate else 0
        is_international = random.random() < self.config.international_rate
        is_high_risk_merchant = random.random() < self.config.high_risk_merchant_rate
        is_velocity_burst = random.random() < self.config.velocity_burst_rate

        merchant_category = random.choice(MERCHANT_CATEGORIES)
        if is_high_risk_merchant:
            merchant_category = random.choice(HIGH_RISK_MERCHANTS)

        amount = round(
            random.uniform(5, 2500)
            if fraud_label == 0
            else random.uniform(500, 15000),
            2,
        )

        country = fake.country_code()
        city = fake.city()
        if is_international:
            country = random.choice(["US", "GB", "IN", "AE", "SG", "DE", "FR"])
            city = fake.city()

        payload = {
            "transaction_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "customer_id": f"CUST_{random.randint(10000, 99999)}",
            "account_id": f"ACC_{random.randint(100000, 999999)}",
            "card_id": f"CARD_{random.randint(1000000, 9999999)}",
            "merchant_id": f"MER_{random.randint(1000, 9999)}",
            "merchant_category": merchant_category,
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "amount": amount,
            "currency": random.choice(CURRENCIES),
            "country": country,
            "city": city,
            "device_id": str(uuid.uuid4())[:12],
            "ip_address": fake.ipv4_public(),
            "channel": random.choice(CHANNELS),
            "is_international": is_international,
            "is_velocity_burst": is_velocity_burst,
            "is_high_risk_merchant": is_high_risk_merchant,
            "baseline_risk_score": self._risk_score(
                amount, is_international, is_high_risk_merchant, is_velocity_burst
            ),
            "fraud_label": fraud_label,
        }

        return payload

    def generate_batch(self, n: int) -> List[Dict]:
        return [self.generate_transaction() for _ in range(n)]