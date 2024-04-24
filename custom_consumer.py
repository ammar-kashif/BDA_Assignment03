from kafka import KafkaConsumer
import json
from collections import Counter
from pymongo import MongoClient

class TrendAnalysisConsumer:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.brand_counts = Counter()
        self.brand_pairs = Counter()
        self.brands_collection = self.db['brands']
        self.pairs_collection = self.db['brand_pairs']

    def clear_collections(self):
        # clearing the brands and pairs collections
        self.brands_collection.delete_many({})
        self.pairs_collection.delete_many({})

        print("Cleared brands and brand_pairs collections.")

    def process_message(self, message):
        products = json.loads(message.value.decode('utf-8'))
        
        # process each product in the message
        for data in products:
            current_brand = data.get('brand', '').strip() or "The Triple A Gs"

            # update brand count in MongoDB
            self.brands_collection.update_one(
                {'brand': current_brand},
                {'$inc': {'count': 1}},
                upsert=True
            )

            related_products = data.get('related', [])
            for related_asin in related_products:
                related_brand = "Related Brand for " + related_asin
                pair = tuple(sorted([current_brand, related_brand]))

                # update brand pair count in MongoDB
                self.pairs_collection.update_one(
                    {'pair': pair},
                    {'$inc': {'count': 1}},
                upsert=True
                )

    def get_brand_by_asin(self, asin):
        # Placeholder for brand name as the brand name may not be available in the stream data
        return "Brand for " + asin
    
    def report(self):
        # get the top 5 brands and brand pairs
        top_brands = self.brands_collection.find().sort("count", -1).limit(5)
        top_pairs = self.pairs_collection.find().sort("count", -1).limit(5)

        print("Top 5 Brands:")
        for brand in top_brands:
            print(f"{brand['brand']}: {brand['count']}")

        print("Top 5 Brand Pairs:")
        for pair in top_pairs:
            print(f"{pair['pair']}: {pair['count']}")


def consume_data(consumer, mongo_uri, db_name):
    # initialize the TrendAnalysisConsumer object
    analysis_consumer = TrendAnalysisConsumer(mongo_uri, db_name)

    # clear the collections in MongoDB
    analysis_consumer.clear_collections()

    # consume the dataset from the Kafka topic
    for message in consumer:
        analysis_consumer.process_message(message)
        analysis_consumer.report()

# declare parameters
bootstrap_servers = 'localhost:9092'
mongo_uri = 'mongodb://localhost:27017/'
topic = 'MarketTrends'

# initialize the consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

# start consuming the dataset
consume_data(consumer, mongo_uri, topic)

consumer.close()