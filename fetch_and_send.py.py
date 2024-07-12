import time
import json
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

URL = "https://scrapeme.live/shop/"
KAFKA_TOPIC = "test-topic"
BOOTSTRAP_SERVERS = 'localhost:9092'

def fetch_data():
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    products = []
    for product in soup.select('.product'):
        name = product.select_one('.woocommerce-loop-product__title').text
        price = product.select_one('.price').text
        description = product.select_one('.woocommerce-product-details__short-description').text.strip()
        stock = product.select_one('.stock').text if product.select_one('.stock') else 'Unknown'
        products.append({
            "name": name,
            "price": price,
            "description": description,
            "stock": stock
        })
    return products

def produce_messages(producer, data):
    for item in data:
        message = json.dumps(item).encode('utf-8')
        producer.send(KAFKA_TOPIC, message)
        time.sleep(1)

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    data = fetch_data()
    produce_messages(producer, data)
    producer.close()
