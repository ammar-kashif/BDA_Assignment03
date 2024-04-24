from kafka import KafkaConsumer
import json
from collections import Counter
from pymongo import MongoClient

class TrendAnalysisConsumer:
    def __init__(self, output_path, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.brand_counts = Counter()
        self.brand_pairs = Counter()  # Store brand pairs instead of product pairs
        self.output_path = output_path
        self.brands_collection = self.db['brands']
        self.pairs_collection = self.db['brand_pairs']

    def clear_collections(self):
        # Clear the brands and pairs collections
        self.brands_collection.delete_many({})
        self.pairs_collection.delete_many({})
        print("Cleared brands and brand_pairs collections.")

    def process_message(self, message):
        products = json.loads(message.value.decode('utf-8'))  # Assuming the entire message is an array of products
        
        for data in products:
            current_brand = data.get('brand', '').strip() or "The Triple A Gs"

            # Update brand count in MongoDB in real time
            self.brands_collection.update_one(
                {'brand': current_brand},
                {'$inc': {'count': 1}},
                upsert=True
            )

            related_products = data.get('related', [])
            for related_asin in related_products:
                related_brand = "Related Brand for " + related_asin  # Placeholder, replace with actual data retrieval logic
                pair = tuple(sorted([current_brand, related_brand]))

                # Update brand pair count in MongoDB in real time
                self.pairs_collection.update_one(
                    {'pair': pair},
                    {'$inc': {'count': 1}},
                upsert=True
                )

    def get_brand_by_asin(self, asin):
        # Placeholder for brand retrieval logic; needs actual implementation based on your data structure or a secondary data source
        return "Brand for " + asin
    
    def report(self):
        top_brands = self.brands_collection.find().sort("count", -1).limit(5)
        top_pairs = self.pairs_collection.find().sort("count", -1).limit(5)

        print("Top 5 Brands:")
        for brand in top_brands:
            print(f"{brand['brand']}: {brand['count']}")

        print("Top 5 Brand Pairs:")
        for pair in top_pairs:
            print(f"{pair['pair']}: {pair['count']}")


def consume_data(topic, bootstrap_servers, mongo_uri, db_name):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')
    analysis_consumer = TrendAnalysisConsumer('output/trending_brands.txt', mongo_uri, db_name)
    analysis_consumer.clear_collections()
    for message in consumer:
        analysis_consumer.process_message(message)
        analysis_consumer.report()
    consumer.close()

bootstrap_servers = 'localhost:9092'
mongo_uri = 'mongodb://localhost:27017/'
db_name = 'MarketTrends'

# Run consumer
consume_data('Custom', bootstrap_servers, mongo_uri, db_name)
