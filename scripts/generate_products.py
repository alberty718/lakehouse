import json
import os
from faker import Faker
import random

fake = Faker('ru_RU')

def generate_products(count=200):
    categories = ["Электроника", "Одежда", "Продукты", "Дом и сад", "Спорт"]
    products = []
    
    for i in range(count):
        products.append({
            "product_id": f"PROD-{1000 + i}",
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "brand": fake.company(),
            "price": round(random.uniform(500, 75000), 2),
            "currency": "RUB"
        })
    return products

if __name__ == "__main__":
    data = generate_products()
    os.makedirs("output", exist_ok=True)
    
    with open("output/products.json", 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    print(f"Сгенерировано {len(data)} товаров в output/products.json")