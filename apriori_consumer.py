from kafka import KafkaConsumer
import json
from collections import defaultdict, Counter
from itertools import combinations

class SlidingApriori:
    def __init__(self, size=100):
        self.window = []
        self.size = size
        self.itemsets = defaultdict(int)

    def add_transaction(self, transaction):
        if len(self.window) >= self.size:
            self.remove_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    def remove_transaction(self):
        old_transaction = self.window.pop(0)
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, inc):
        max_length = 3  # typically, 2 or 3 might be more practical
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                self.itemsets[tuple(sorted(itemset))] += inc
                if self.itemsets[tuple(sorted(itemset))] <= 0:
                    del self.itemsets[tuple(sorted(itemset))]

    def generate_frequent_itemsets(self, min_support):
        return {itemset: count for itemset, count in self.itemsets.items() if count >= min_support}

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

def consume_dataset(consumer, sliding_apriori, min_support=2, min_confidence=0.5):
    for message in consumer:
        try:
            dataset = json.loads(message.value.decode("utf-8"))
            transactions = [[item["asin"]] + item.get("related", []) for item in dataset]
            for transaction in transactions:
                sliding_apriori.add_transaction(transaction)

            print("Processing frequent itemsets and association rules...")
            frequent_itemsets = sliding_apriori.generate_frequent_itemsets(min_support)
            write_frequent_itemsets(frequent_itemsets, "output/apriori_frequent_itemsets.txt")

            association_rules = sliding_apriori.generate_association_rules(min_confidence)
            write_association_rules(association_rules, "output/apriori_association_rules.txt")
            print("Results written to output files.")

        except json.JSONDecodeError:
            print("Failed to decode JSON from message")
        except Exception as e:
            print(f"Unexpected error: {str(e)}")

def write_frequent_itemsets(frequent_itemsets, output_path):
    with open(output_path, "w") as file:
        file.write("Itemset\tSupport\n")
        for itemset, count in frequent_itemsets.items():
            file.write(f"{' ,'.join(itemset)}\t{count}\n")

def write_association_rules(association_rules, output_path):
    with open(output_path, "w") as file:
        file.write("Antecedent\tConsequent\tConfidence\n")
        for antecedent, consequent, confidence in association_rules:
            file.write(f"{' ,'.join(antecedent)}\t{' ,'.join(consequent)}\t{confidence:.2f}\n")

# Setup
bootstrap_servers = "localhost:9092"
topic = "Apriori"
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

# Main
sliding_apriori = SlidingApriori(size=100)
consume_dataset(consumer, sliding_apriori)

# Cleanup
consumer.close()
