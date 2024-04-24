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
        # clearing the itemsets and rules collections
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
        max_length = 3

        # update the counts of itemsets
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                self.itemsets[tuple(sorted(itemset))] += inc
                if self.itemsets[tuple(sorted(itemset))] <= 0:
                    del self.itemsets[tuple(sorted(itemset))]

    def save_to_db(self, min_support, min_confidence):
        # save frequent itemsets to MongoDB
        frequent_itemsets = {itemset: count for itemset, count in self.itemsets.items() if count >= min_support}
        for itemset, count in frequent_itemsets.items():
            self.itemsets_collection.update_one(
                {'itemset': itemset},
                {'$set': {'count': count}},
                upsert=True
            )

        # save association rules to MongoDB
        rules = self.generate_association_rules(min_confidence)
        for rule in rules:
            self.rules_collection.update_one(
                {'rule': rule},
                {'$set': {'confidence': rule[2]}},
                upsert=True
            )

    def generate_association_rules(self, min_confidence):
        association_rules = []

        # generate association rules from frequent itemsets
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
        
        # return the association rules
        return association_rules

def consume_dataset(consumer, mongo_uri, db_name):
    # declare parameters
    min_support = 3
    min_confidence = 0.5

    # initialize the SlidingApriori class
    sliding_apriori = SlidingApriori(mongo_uri, db_name)
    
    # clear the collections in MongoDB
    sliding_apriori.clear_collections()

    # consume the dataset
    for message in consumer:
        dataset = json.loads(message.value.decode("utf-8"))
        transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
        for transaction in transactions:
            sliding_apriori.add_transaction(transaction)

        # save results to MongoDB
        sliding_apriori.save_to_db(min_support, min_confidence)
        print("Saved frequent itemsets and association rules to MongoDB.")

# declare parameters
bootstrap_servers = 'localhost:9092'
mongo_uri = 'mongodb://localhost:27017'
topic = 'Apriori'

# initialize the consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

# start consuming the dataset
consume_dataset(consumer, 'mongodb://localhost:27017', topic)

consumer.close()