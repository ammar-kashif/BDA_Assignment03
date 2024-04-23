from kafka import KafkaConsumer
import json
from collections import Counter
from itertools import combinations

class SlidingPCY:
    def __init__(self, num_buckets, hash_support, min_support, window_size):
        self.num_buckets = num_buckets
        self.hash_support = hash_support
        self.min_support = min_support
        self.window_size = window_size
        self.single_counts = Counter()
        self.pair_counts = Counter()
        self.triplet_counts = Counter()
        self.hash_table = [0] * num_buckets
        self.window = []

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
        # Check for pairs
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

        # Check for triplets
        for triplet, triplet_support in self.triplet_counts.items():
            if triplet_support >= self.min_support:
                for antecedent in combinations(triplet, 2):
                    consequent = tuple(set(triplet) - set(antecedent))
                    antecedent_support = self.pair_counts.get(antecedent, 0)
                    if antecedent_support > 0:
                        confidence = triplet_support / antecedent_support
                        if confidence >= min_confidence:
                            rules.append((antecedent, consequent, confidence))

        return rules

def consume_dataset(consumer, sliding_pcy, output_paths):
    with open(output_paths['itemsets'], 'w') as f_itemsets, open(output_paths['rules'], 'w') as f_rules:
        for message in consumer:
            dataset = json.loads(message.value.decode('utf-8'))
            transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
            for transaction in transactions:
                sliding_pcy.process_transaction(transaction)

            # Output results to files
            print("Processing frequent itemsets and association rules...")
            write_frequent_itemsets(sliding_pcy, f_itemsets)
            write_association_rules(sliding_pcy.generate_association_rules(0.5), f_rules)
            print("Results written to output files.")

def write_frequent_itemsets(pcy_instance, file):
    file.write("Frequent Singles:\n")
    for item, count in pcy_instance.single_counts.items():
        if count >= pcy_instance.min_support:
            file.write(f"{item}\t{count}\n")

    file.write("\nFrequent Pairs:\n")
    for pair, count in pcy_instance.pair_counts.items():
        if count >= pcy_instance.min_support:
            file.write(f"{pair}\t{count}\n")

    file.write("\nFrequent Triplets:\n")
    for triplet, count in pcy_instance.triplet_counts.items():
        if count >= pcy_instance.min_support:
            file.write(f"{triplet}\t{count}\n")

def write_association_rules(rules, file):
    for antecedent, consequent, confidence in rules:
        antecedent_str = ', '.join(map(str, antecedent))  # Converting tuples to string
        consequent_str = ', '.join(map(str, consequent))
        file.write(f"Antecedent: [{antecedent_str}], Consequent: [{consequent_str}], Confidence: {confidence:.2f}\n")

# Set up Kafka consumer and PCY instance
bootstrap_servers = 'localhost:9092'
topic = 'PCY'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')
output_paths = {
    'itemsets': 'output/pcy_frequent_itemsets.txt',
    'rules': 'output/pcy_association_rules.txt'
}

# Clear output files
open(output_paths['itemsets'], 'w').close()
open(output_paths['rules'], 'w').close()

sliding_pcy = SlidingPCY(num_buckets=10, hash_support=2, min_support=3, window_size=100)
consume_dataset(consumer, sliding_pcy, output_paths)
consumer.close()
