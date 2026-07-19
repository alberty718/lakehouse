import json
import os
from faker import Faker
from datetime import datetime

fake = Faker('ru_RU')

def generate_customers(count=1000):
    customers = []
    for _ in range(count):
        customers.append({
            "customer_id": str(fake.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "city": fake.city(),
            "segment": fake.random_element(["premium", "standard", "basic"]),
            "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat()
        })
    return customers

if __name__ == "__main__":
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    data = generate_customers(1000)
    filename = f"{output_dir}/crm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Generated {len(data)} customers to {filename}")