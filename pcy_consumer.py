from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from collections import Counter
from itertools import combinations

class SlidingPCY:
    def __init__(self, mongo_uri, db_name, num_buckets, hash_support, min_support, window_size):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.itemsets_collection = self.db['itemsets']
        self.rules_collection = self.db['rules']
        self.num_buckets = num_buckets
        self.hash_support = hash_support
        self.min_support = min_support
        self.window_size = window_size
        self.single_counts = Counter()
        self.pair_counts = Counter()
        self.triplet_counts = Counter()
        self.hash_table = [0] * num_buckets
        self.window = []

    def clear_collections(self):
        # clearing the brands and pairs collections
        self.itemsets_collection.delete_many({})
        self.rules_collection.delete_many({})

        print("Cleared itemsets and rules collections.")

    def hash_bucket(self, itemset):
        return hash(itemset) % self.num_buckets

    def process_transaction(self, transaction):
        if len(self.window) >= self.window_size:
            self.remove_oldest_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    def remove_oldest_transaction(self):
        old_transaction = self.window.pop(0)
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, increment):
        # update the counts of single, pair, and triplet items
        for item in transaction:
            self.single_counts[item] += increment
        for pair in combinations(transaction, 2):
            bucket = self.hash_bucket(pair)
            self.hash_table[bucket] += increment
            if self.hash_table[bucket] >= self.hash_support:
                self.pair_counts[pair] += increment
        for triplet in combinations(transaction, 3):
            if all(self.pair_counts[pair] >= self.min_support for pair in combinations(triplet, 2)):
                self.triplet_counts[triplet] += increment

    def generate_association_rules(self, min_confidence):
        rules = []
        # generate association rules from frequent pairs
        for pair, pair_support in self.pair_counts.items():
            if pair_support >= self.min_support:
                for item in pair:
                    antecedent = (item,)
                    consequent = tuple(set(pair) - set(antecedent))
                    antecedent_support = self.single_counts[item]
                    if antecedent_support > 0:
                        confidence = pair_support / antecedent_support
                        if confidence >= min_confidence:
                            rules.append((antecedent, consequent, confidence))
        
        # return the association rules
        return rules
    
    def save_to_db(self):
        # save frequent itemsets to MongoDB
        for itemset, count in self.single_counts.items():
            if count >= self.min_support:
                self.itemsets_collection.update_one(
                    {'itemset': str(itemset)},
                    {'$set': {'count': count}},
                    upsert=True
                )

        for itemset, count in self.pair_counts.items():
            if count >= self.min_support:
                self.itemsets_collection.update_one(
                    {'itemset': str(itemset)},
                    {'$set': {'count': count}},
                    upsert=True
                )

        for itemset, count in self.triplet_counts.items():
            if count >= self.min_support:
                self.itemsets_collection.update_one(
                    {'itemset': str(itemset)},
                    {'$set': {'count': count}},
                    upsert=True
                )

        # save association rules to MongoDB
        rules = self.generate_association_rules(0.5)
        for rule in rules:
            self.rules_collection.update_one(
                {'rule': str(rule)},
                {'$set': {'confidence': rule[2]}},
                upsert=True
            )

def consume_dataset(consumer, mongo_uri, db_name):
    # declare parameters
    num_buckets = 10
    hash_support = 2
    min_support = 3
    window_size = 100

    # initialize the SlidingPCY object
    sliding_pcy = SlidingPCY(mongo_uri, db_name, num_buckets, hash_support, min_support, window_size)
    
    # clear the collections in MongoDB
    sliding_pcy.clear_collections()

    # consume the dataset from the Kafka topic
    for message in consumer:
        dataset = json.loads(message.value.decode('utf-8'))
        transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
        for transaction in transactions:
            sliding_pcy.process_transaction(transaction)
        sliding_pcy.save_to_db()
        print("Saved frequent itemsets and association rules to MongoDB.")
    
# declare parameters
bootstrap_servers = 'localhost:9092'
mongo_uri = 'mongodb://localhost:27017'
topic = 'PCY'

# initialize the consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

# start consuming the dataset
consume_dataset(consumer, 'mongodb://localhost:27017', topic)

consumer.close()
