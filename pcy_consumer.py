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
        # Adding transaction to the window
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
        # Removing transaction from the window
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
            # Process transactions
            self.add_transaction(transaction)
            if len(self.window) > self.window_size:
                self.remove_transaction(self.window[0])

        return (self.filter_items(self.single_counts), 
                self.filter_items(self.pair_counts),
                self.filter_items(self.triplet_counts))

    def filter_items(self, counts):
        return {itemset: count for itemset, count in counts.items() if count >= self.min_support}

    def generate_association_rules(self, min_confidence):
        
        association_rules = []
        # Generate association rules
        for pair, support in self.pair_counts.items():
            antecedent, consequent = tuple(pair)
            antecedent_support = self.single_counts[antecedent]
            if antecedent_support == 0:
                continue
            confidence = support / antecedent_support
            if confidence >= min_confidence:
                association_rules.append((antecedent, consequent, confidence))

        # Generate association rules for frequent triplets
        for triplet, support in self.triplet_counts.items():
            for i in range(1, len(triplet)):
                for antecedent in combinations(triplet, i):
                    antecedent = tuple(sorted(antecedent))
                    consequent = tuple(sorted(set(triplet) - set(antecedent)))
                    antecedent_support = self.pair_counts[antecedent]
                    if antecedent_support == 0:
                        continue
                    confidence = support / antecedent_support
                    if confidence >= min_confidence:
                        association_rules.append((antecedent, consequent, confidence))

        return association_rules

def consume_dataset(consumer, sliding_pcy):
    for message in consumer:
        dataset = json.loads(message.value.decode('utf-8'))
        transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
        frequent_singles, frequent_pairs, frequent_triplets = sliding_pcy.process_transactions(transactions)

        # Generate association rules
        min_confidence = 0.5
        association_rules = sliding_pcy.generate_association_rules(min_confidence)

        # Output frequent items and association rules
        output_results(frequent_singles, frequent_pairs, frequent_triplets, association_rules)

def output_results(singles, pairs, triplets, association_rules):
    frequent_itemsets_output_path = 'pcy_frequent_itemsets.txt'
    association_rules_output_path = 'PCY_Association_Rules.txt'

    # Clear output files when consumer starts
    with open(frequent_itemsets_output_path, "w") as frequent_file:
        frequent_file.write("")

    with open(association_rules_output_path, "w") as association_file:
        association_file.write("")

    # Writing frequent itemsets to the frequent itemsets output file
    with open(frequent_itemsets_output_path, 'w') as file:
        file.write("Frequent Singles:\n")
        for item, count in singles.items():
            file.write(f"{item}\t{count}\n")

        file.write("\nFrequent Pairs:\n")
        for pair, count in pairs.items():
            file.write(f"{pair}\t{count}\n")

        file.write("\nFrequent Triplets:\n")
        for triplet, count in triplets.items():
            file.write(f"{triplet}\t{count}\n")

    # Writing association rules to the association rules output file
    with open(association_rules_output_path, 'w') as file:
        file.write("Association Rules:\n")
        for antecedent, consequent, confidence in association_rules:
            file.write(f"Antecedent: {antecedent}, Consequent: {consequent}, Confidence: {confidence}\n")

    print("Frequent itemsets saved to", frequent_itemsets_output_path)
    print("Association rules saved to", association_rules_output_path)


bootstrap_servers = 'localhost:9092'
topic = 'PCY'
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Create SlidingPCY instance and consume dataset
sliding_pcy = SlidingPCY(num_buckets=10, hash_support=2, min_support=3, window_size=100)

consume_dataset(consumer, sliding_pcy)

consumer.close()
