from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from collections import defaultdict, Counter
from itertools import combinations

class SlidingApriori:
    def __init__(self, mongo_uri, db_name, size=100):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.itemsets_collection = self.db['apriori_itemsets']
        self.rules_collection = self.db['apriori_rules']
        self.window = []
        self.size = size
        self.itemsets = defaultdict(int)

    def clear_collections(self):
        self.itemsets_collection.delete_many({})
        self.rules_collection.delete_many({})
        print("Cleared itemsets and rules collections in MongoDB.")

    def add_transaction(self, transaction):
        if len(self.window) >= self.size:
            self.remove_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    def remove_transaction(self):
        old_transaction = self.window.pop(0)
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, inc):
        max_length = 3  # typically, 2 or 3 might be practical
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                self.itemsets[tuple(sorted(itemset))] += inc
                if self.itemsets[tuple(sorted(itemset))] <= 0:
                    del self.itemsets[tuple(sorted(itemset))]

    def save_to_db(self, min_support, min_confidence):
        # Save frequent itemsets
        frequent_itemsets = {itemset: count for itemset, count in self.itemsets.items() if count >= min_support}
        for itemset, count in frequent_itemsets.items():
            print("this")
            self.itemsets_collection.update_one(
                {'itemset': itemset},
                {'$set': {'count': count}},
                upsert=True
            )

        # Save association rules
        rules = self.generate_association_rules(min_confidence)
        for rule in rules:
            print("that")
            self.rules_collection.update_one(
                {'rule': rule},
                {'$set': {'confidence': rule[2]}},
                upsert=True
            )

    def generate_association_rules(self, min_confidence):
        association_rules = []
        for itemset, support in self.itemsets.items():
            if len(itemset) > 1:
                for i in range(1, len(itemset)):
                    for antecedent in combinations(itemset, i):
                        consequent = tuple(sorted(set(itemset) - set(antecedent)))
                        antecedent_support = self.itemsets[antecedent]
                        if antecedent_support > 0:
                            confidence = support / antecedent_support
                            if confidence >= min_confidence:
                                association_rules.append((antecedent, consequent, confidence))
        return association_rules

def consume_dataset(topic, bootstrap_servers, mongo_uri, db_name):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')
    sliding_apriori = SlidingApriori(mongo_uri, db_name)
    sliding_apriori.clear_collections()  # Clear collections each time the consumer starts

    for message in consumer:
        dataset = json.loads(message.value.decode("utf-8"))
        transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
        for transaction in transactions:
            sliding_apriori.add_transaction(transaction)

        # Save results to MongoDB
        sliding_apriori.save_to_db(3, 0.5)  # min_support and min_confidence as example
        print("Saved frequent itemsets and association rules to MongoDB.")

    consumer.close()

# Example usage
consume_dataset('Apriori', 'localhost:9092', 'mongodb://localhost:27017', 'Apriori')
