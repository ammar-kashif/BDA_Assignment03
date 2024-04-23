from kafka import KafkaConsumer
import json
from collections import defaultdict, Counter
from itertools import combinations

class SlidingApriori:
    def __init__(self, size=100):
        self.window = []
        self.size = size
        self.itemsets = defaultdict(int)

    # Adding a transaction to the sliding window
    def add_transaction(self, transaction):
        if len(self.window) >= self.size:
            self.remove_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    # Removing the oldest transaction from the sliding window
    def remove_transaction(self):
        old_transaction = self.window.pop(0)
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, inc):
        max_length = 4
        # Updating counts
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                # Convert itemset to tuple to avoid frozenset
                itemset_tuple = tuple(sorted(itemset))
                self.itemsets[itemset_tuple] += inc
                if self.itemsets[itemset_tuple] <= 0:
                    del self.itemsets[itemset_tuple]

    def generate_itemsets(self, transactions, min_support):
        single_counts = Counter()
        pair_counts = Counter()
        triplet_counts = Counter()
        total_transactions = len(transactions)

        # Count single items, pairs, and triplets
        for transaction in transactions:
            single_counts.update(transaction)
            for pair in combinations(transaction, 2):
                pair_counts[frozenset(pair)] += 1
            for triplet in combinations(transaction, 3):
                triplet_counts[frozenset(triplet)] += 1

        # Filtering frequent itemsets based on minimum support
        frequent_singles = {
            item: count for item, count in single_counts.items() if count >= min_support
        }
        frequent_pairs = {
            pair: count for pair, count in pair_counts.items() if count >= min_support
        }
        frequent_triplets = {
            triplet: count
            for triplet, count in triplet_counts.items()
            if count >= min_support
        }

        # Outputing frequent itemsets to a file
        with open("frequent_itemsets.txt", "w") as file:
            file.write("Item\tSupport\n")
            for item, count in frequent_singles.items():
                file.write(f"{item}\t{count}\n")

            for pair, count in frequent_pairs.items():
                file.write(f"{pair}\t{count}\n")

            for triplet, count in frequent_triplets.items():
                file.write(f"{triplet}\t{count}\n")

        return frequent_singles, frequent_pairs, frequent_triplets

    def generate_association_rules(
        self, frequent_pairs, frequent_triplets, transactions, min_confidence
    ):
        association_rules = []

        # Generating association rules
        for pair, support in frequent_pairs.items():
            antecedent, consequent = tuple(pair)
            antecedent_support = self.itemsets[antecedent]
            if antecedent_support == 0:
                continue
            confidence = support / antecedent_support
            if confidence >= min_confidence:
                association_rules.append((antecedent, consequent, confidence))

        # Generating association rules for frequent triplets
        for triplet, support in frequent_triplets.items():
            for i in range(1, len(triplet)):
                for antecedent in combinations(triplet, i):
                    antecedent = tuple(sorted(antecedent))
                    consequent = tuple(sorted(set(triplet) - set(antecedent)))
                    antecedent_support = self.itemsets[antecedent]
                    if antecedent_support == 0:
                        continue
                    confidence = support / antecedent_support
                    if confidence >= min_confidence:
                        association_rules.append((antecedent, consequent, confidence))

        return association_rules


def consume_dataset(consumer, output_path):
    print("Consumer started. Waiting for dataset from producer...")

    dataset_received = False

    # Reading the entire dataset from Kafka
    for message in consumer:
        dataset = json.loads(message.value.decode("utf-8"))
        print("Dataset received from producer.")
        dataset_received = True
        break

    if not dataset_received:
        print("No dataset received. Exiting...")
        return

    # Extract transactions from the dataset
    transactions = []
    for item in dataset:
        # Getting the asin and related items for each transaction
        transaction = [item["asin"]] + item.get("related", [])
        transactions.append(transaction)
    print("Transactions extracted from the dataset.")

    # Creating a SlidingApriori instance
    sliding_window = SlidingApriori(size=len(transactions))
    print("Sliding window initialized.")

    # Adding transactions to the sliding window
    for transaction in transactions:
        sliding_window.add_transaction(transaction)
    print("Transactions added to sliding window.")

    min_supp = 2

    # Generating frequent itemsets
    frequent_singles, frequent_pairs, frequent_triplets = (
        sliding_window.generate_itemsets(transactions, min_supp)
    )
    print("Frequent itemsets generated.")

    # Generate association rules
    min_conf = 0.5
    association_rules = sliding_window.generate_association_rules(
        frequent_pairs, frequent_triplets, transactions, min_conf
    )
    print("\nAssociation rules generated.")

    # Saving frequent itemsets to a file
    with open(output_path, "w") as file:
        file.write("Itemset\tSupport\n")

        for item, count in frequent_singles.items():
            file.write(f"{item}\t{count}\n")

        for pair, count in frequent_pairs.items():
            file.write(f"{', '.join(pair)}\t{count}\n")

        for triplet, count in frequent_triplets.items():
            file.write(f"{', '.join(triplet)}\t{count}\n")

    print("Frequent itemsets saved to", output_path)

    # Saving association rules to file
    with open("Apriori_Association_Rules.txt", "w") as file:
        file.write("Antecedent\tConsequent\tConfidence\n")
        for antecedent, consequent, confidence in association_rules:
            file.write(f"{antecedent}\t{consequent}\t{confidence:.2f}\n")

    print("Association rules saved to Apriori_Association_Rules.txt")


bootstrap_servers = "localhost:9092"
topic = "Apriori"

# Creating Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

output_path = "apriori_frequent_itemsets.txt"

# Clear file
with open(output_path, "w") as file:
    file.write("")

# Consume dataset and generate frequent itemsets
consume_dataset(consumer, output_path)

consumer.close()
