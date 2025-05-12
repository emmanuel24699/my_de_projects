import random
import time
from datetime import datetime
import uuid

def generate_heart_rate_data(num_customers=10):
    """Generate synthetic heart rate data for a fixed pool of customers."""
    customer_ids = [str(uuid.uuid4())[:8] for _ in range(num_customers)]
    while True:
        customer_id = random.choice(customer_ids)
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        heart_rate = random.randint(60, 100)  #
        yield {
            "customer_id": customer_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        }
        time.sleep(1)  # Simulate real-time data every second
