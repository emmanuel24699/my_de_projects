import uuid
import time
from datetime import datetime, timedelta
import pandas as pd
import random
import os

# Configuration
OUTPUT_DIR = "data/input/"
CATALOG_DIR = "data/catalog/"
BATCH_SIZE = 15
SLEEP_INTERVAL = 10
EVENT_TYPES = ["view", "click", "buy"]
CATEGORIES = ["Electronics", "Fashion", "Books", "Home & Kitchen", "Toys"]

# Reduced product list
PRODUCTS = [
    {"brand": "Apple", "name": "iPhone 14", "category": "Electronics", "price": 999.99},
    {"brand": "Nike", "name": "Air Max", "category": "Fashion", "price": 129.99},
    {"brand": "Penguin", "name": "1984", "category": "Books", "price": 14.99},
    {"brand": "Dyson", "name": "V15 Vacuum", "category": "Home & Kitchen", "price": 699.99},
    {"brand": "LEGO", "name": "Star Wars Set", "category": "Toys", "price": 159.99}
]

# Generate product catalog
products = [{"product_id": f"prod_{str(i).zfill(6)}", **prod} for i, prod in enumerate(PRODUCTS, 1)]

# Generate users
NUM_USERS = 1000
users = [f"user_{str(i).zfill(6)}" for i in range(NUM_USERS)]

def save_product_catalog():
    pd.DataFrame(products).to_csv(f"{CATALOG_DIR}/products.csv", index=False)

def generate_session_events():
    session_id = str(uuid.uuid4())
    user_id = random.choice(users)
    product = random.choice(products)
    base_time = datetime.utcnow() - timedelta(seconds=random.uniform(0, 24 * 60 * 60))
    events = []

    # View event
    events.append({
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": "view",
        **product,
        "quantity": 1,
        "timestamp": (base_time).isoformat() + "Z"
    })

    # Click event (20% chance)
    if random.random() < 0.2:
        click_time = base_time + timedelta(seconds=random.uniform(10, 300))
        events.append({
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "event_type": "click",
            **product,
            "quantity": 1,
            "timestamp": click_time.isoformat() + "Z"
        })

        # Buy event (15% chance after click)
        if random.random() < 0.15:
            buy_time = click_time + timedelta(seconds=random.uniform(120, 43200))
            events.append({
                "event_id": str(uuid.uuid4()),
                "session_id": session_id,
                "user_id": user_id,
                "event_type": "buy",
                **product,
                "quantity": random.randint(1, 5),
                "timestamp": buy_time.isoformat() + "Z"
            })

    return events

def generate_batch(batch_size):
    events = []
    while len(events) < batch_size:
        events.extend(generate_session_events())
    return events[:batch_size]

def save_batch(events):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")
    pd.DataFrame(events).to_csv(f"{OUTPUT_DIR}/events_{timestamp}.csv", index=False)

def main():
    save_product_catalog()
    while True:
        save_batch(generate_batch(BATCH_SIZE))
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped data generator.")