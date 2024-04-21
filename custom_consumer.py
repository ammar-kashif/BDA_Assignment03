from kafka import KafkaConsumer
import json
from collections import Counter

class TrendingBrandsConsumer:
    def __init__(self, output_path, window_size):
        self.window_size = window_size
        self.brand_counts = Counter()
        self.transaction_window = []
        self.output_path = output_path

    def add_transaction(self, transaction):
        brand = transaction.get('brand')
        if brand:
            self.transaction_window.append(brand)
            self.brand_counts[brand] += 1
        if len(self.transaction_window) > self.window_size:
            old_brand = self.transaction_window.pop(0)
            self.brand_counts[old_brand] -= 1
            if self.brand_counts[old_brand] == 0:
                del self.brand_counts[old_brand]

    def consume_dataset(self, consumer):
        for message in consumer:
            dataset = json.loads(message.value.decode('utf-8'))
            for item in dataset:
                self.add_transaction(item)
            self.output_trending_brands()

    def output_trending_brands(self):
        # Sort brands by count and get the top 5
        top_brands = self.brand_counts.most_common(5)
        with open(self.output_path, 'w') as file:
            file.write("Top 5 Trending Brands:\n")
            for brand, count in top_brands:
                file.write(f"{brand}\t{count}\n")
        print(f"Results saved to {self.output_path}")

# Kafka settings and consumer initialization
bootstrap_servers = 'localhost:9092'
topic = 'Custom'  # Use the same or different topic as needed

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

output_path = 'output/trending_brands.txt'

# Clear output file when consumer starts
with open(output_path, "w") as file:
    file.write("")

# Create TrendingBrandsConsumer instance and consume dataset
trending_consumer = TrendingBrandsConsumer(output_path, window_size=100)  # Set the window size
trending_consumer.consume_dataset(consumer)

# Close the consumer when done
consumer.close()
