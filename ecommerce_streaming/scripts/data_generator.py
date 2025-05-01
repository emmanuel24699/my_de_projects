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

# Define products with specific brand-product pairs
PRODUCTS = [
    # Electronics
    {"brand": "Apple", "name": "iPhone 14 Pro", "category": "Electronics", "price": 999.99},
    {"brand": "Apple", "name": "MacBook Air M2", "category": "Electronics", "price": 1199.99},
    {"brand": "Samsung", "name": "Galaxy S23 Ultra", "category": "Electronics", "price": 1199.99},
    {"brand": "Sony", "name": "WH-1000XM5 Headphones", "category": "Electronics", "price": 349.99},
    {"brand": "Dell", "name": "XPS 13 Laptop", "category": "Electronics", "price": 1299.99},
    # Fashion
    {"brand": "Nike", "name": "Air Max Sneakers", "category": "Fashion", "price": 129.99},
    {"brand": "Zara", "name": "Leather Jacket", "category": "Fashion", "price": 199.99},
    {"brand": "Adidas", "name": "Ultraboost Running Shoes", "category": "Fashion", "price": 149.99},
    {"brand": "Gucci", "name": "Silk Dress", "category": "Fashion", "price": 249.99},
    {"brand": "Levi's", "name": "501 Slim Fit Jeans", "category": "Fashion", "price": 59.99},
    # Books
    {"brand": "Penguin", "name": "Pride and Prejudice", "category": "Books", "price": 12.99},
    {"brand": "Random House", "name": "Dune", "category": "Books", "price": 19.99},
    {"brand": "Scholastic", "name": "Harry Potter Series", "category": "Books", "price": 49.99},
    {"brand": "HarperCollins", "name": "The Hobbit", "category": "Books", "price": 15.99},
    {"brand": "Penguin", "name": "1984", "category": "Books", "price": 14.99},
    # Home & Kitchen
    {"brand": "Dyson", "name": "V15 Vacuum Cleaner", "category": "Home & Kitchen", "price": 699.99},
    {"brand": "Cuisinart", "name": "Non-Stick Cookware Set", "category": "Home & Kitchen", "price": 199.99},
    {"brand": "Le Creuset", "name": "Cast Iron Dutch Oven", "category": "Home & Kitchen", "price": 399.99},
    {"brand": "IKEA", "name": "Billy Bookcase", "category": "Home & Kitchen", "price": 79.99},
    {"brand": "Cuisinart", "name": "Coffee Maker", "category": "Home & Kitchen", "price": 99.99},
    # Toys
    {"brand": "LEGO", "name": "Star Wars Millennium Falcon Set", "category": "Toys", "price": 159.99},
    {"brand": "Mattel", "name": "Barbie Dreamhouse", "category": "Toys", "price": 199.99},
    {"brand": "Hasbro", "name": "Monopoly Board Game", "category": "Toys", "price": 29.99},
    {"brand": "Nerf", "name": "Elite Blaster", "category": "Toys", "price": 39.99},
    {"brand": "LEGO", "name": "City Police Station Set", "category": "Toys", "price": 99.99}
]

# Generate product catalog with unique IDs
products = []
for i, prod in enumerate(PRODUCTS, 1):
    products.append({
        "product_id": f"prod_{str(i).zfill(6)}",
        "product_name": prod["name"],
        "brand": prod["brand"],
        "category": prod["category"],
        "price": prod["price"]
    })

# Generate persistent users
NUM_USERS = 1000
users = [f"user_{str(i).zfill(6)}" for i in range(NUM_USERS)]

def save_product_catalog():
    """Save the product catalog to a CSV."""
    if not os.path.exists(CATALOG_DIR):
        os.makedirs(CATALOG_DIR)
    df = pd.DataFrame(products)
    df.to_csv(f"{CATALOG_DIR}/products.csv", index=False)
    print(f"Saved product catalog to {CATALOG_DIR}/products.csv")

def generate_session_events():
    """Generate events for a user session, simulating view -> click -> buy funnel with realistic timestamps."""
    session_id = str(uuid.uuid4())
    user_id = random.choice(users)
    product = random.choice(products)
    events = []

    # Generate base timestamp (within past 24 hours)
    base_time = datetime.utcnow() - timedelta(seconds=random.uniform(0, 24 * 60 * 60))
    
    # View event (always occurs)
    view_time = base_time
    events.append({
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": "view",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "brand": product["brand"],
        "category": product["category"],
        "price": product["price"],
        "quantity": 1,
        "timestamp": view_time.isoformat() + "Z"
    })

    # Click event (20% chance)
    if random.random() < 0.2:
        click_time = view_time + timedelta(seconds=random.uniform(10, 300))
        events.append({
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "event_type": "click",
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "brand": product["brand"],
            "category": product["category"],
            "price": product["price"],
            "quantity": 1,
            "timestamp": click_time.isoformat() + "Z"
        })

        # Buy event (15% chance after click, so ~3% overall)
        if random.random() < 0.15:
            buy_time = click_time + timedelta(seconds=random.uniform(120, 43200))
            events.append({
                "event_id": str(uuid.uuid4()),
                "session_id": session_id,
                "user_id": user_id,
                "event_type": "buy",
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "brand": product["brand"],
                "category": product["category"],
                "price": product["price"],
                "quantity": random.randint(1, 5),
                "timestamp": buy_time.isoformat() + "Z"
            })

    return events

def generate_batch(batch_size):
    """Generate a batch of events with sessions."""
    events = []
    while len(events) < batch_size:
        session_events = generate_session_events()
        events.extend(session_events)
    return events[:batch_size]  # Trim to exact batch size

def save_batch(events):
    """Save events to a timestamped CSV file."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")
    filename = f"{OUTPUT_DIR}/events_{timestamp}.csv"
    
    df = pd.DataFrame(events)
    df.to_csv(filename, index=False)
    print(f"Saved {len(events)} events to {filename}")

def main():
    """Generate events continuously."""
    print("Starting data generator...")
    save_product_catalog()
    try:
        while True:
            events = generate_batch(BATCH_SIZE)
            save_batch(events)
            time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        print("Stopped data generator.")

if __name__ == "__main__":
    main()