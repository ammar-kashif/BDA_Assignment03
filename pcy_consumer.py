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

    def add_transaction(self, transaction):
        self.window.append(transaction)
        for item in transaction:
            self.single_counts[item] += 1
        for pair in combinations(transaction, 2):
            bucket = self.hash_bucket(pair)
            self.hash_table[bucket] += 1
            if self.hash_table[bucket] >= self.hash_support:
                self.pair_counts[pair] += 1
        for triplet in combinations(transaction, 3):
            if all(self.pair_counts[pair] >= self.min_support for pair in combinations(triplet, 2)):
                self.triplet_counts[triplet] += 1

    def remove_transaction(self, transaction):
        self.window.remove(transaction)
        for item in transaction:
            self.single_counts[item] -= 1
        for pair in combinations(transaction, 2):
            bucket = self.hash_bucket(pair)
            self.hash_table[bucket] -= 1
            self.pair_counts[pair] -= 1
        for triplet in combinations(transaction, 3):
            self.triplet_counts[triplet] -= 1

    def process_transactions(self, transactions):
        for transaction in transactions:
            self.add_transaction(transaction)
            if len(self.window) > self.window_size:
                self.remove_transaction(self.window[0])

        return (self.filter_items(self.single_counts), 
                self.filter_items(self.pair_counts),
                self.filter_items(self.triplet_counts))

    def filter_items(self, counts):
        return {itemset: count for itemset, count in counts.items() if count >= self.min_support}

def consume_dataset(consumer, sliding_pcy):
    for message in consumer:
        dataset = json.loads(message.value.decode('utf-8'))
        transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
        frequent_singles, frequent_pairs, frequent_triplets = sliding_pcy.process_transactions(transactions)

        # Output results
        output_results(frequent_singles, frequent_pairs, frequent_triplets)

def output_results(singles, pairs, triplets):
    output_path = 'output/pcy_frequent_itemsets.txt'

    # Clear output file when consumer starts
    with open(output_path, "w") as file:
        file.write("")

    with open(output_path, 'w') as file:
        file.write("Frequent Singles:\n")
        for item, count in singles.items():
            file.write(f"{item}\t{count}\n")

        file.write("\nFrequent Pairs:\n")
        for pair, count in pairs.items():
            file.write(f"{pair}\t{count}\n")

        file.write("\nFrequent Triplets:\n")
        for triplet, count in triplets.items():
            file.write(f"{triplet}\t{count}\n")

    print("Results saved to", output_path)

# Kafka settings and consumer initialization
bootstrap_servers = 'localhost:9092'
topic = 'PCY'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Create SlidingPCY instance and consume dataset
sliding_pcy = SlidingPCY(num_buckets=10, hash_support=2, min_support=3, window_size=100)

while True:
    consume_dataset(consumer, sliding_pcy)