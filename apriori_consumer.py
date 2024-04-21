from kafka import KafkaConsumer
import json
from collections import defaultdict
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
                self.itemsets[itemset] += inc
                if self.itemsets[itemset] <= 0:
                    del self.itemsets[itemset]

    def generate_itemsets(self, transactions, min_support):
        pair_counts = {}
        total_transactions = len(transactions)
        for transaction in transactions:
            for item1, item2 in combinations(set(transaction), 2):
                pair = frozenset([item1, item2])
                if pair in pair_counts:
                    pair_counts[pair] += 1
                else:
                    pair_counts[pair] = 1

        # Apply minimum support threshold
        frequent_itemsets = {pair: count for pair, count in pair_counts.items() if count >= min_support}
        return frequent_itemsets

def consume_dataset(consumer, output_path):
    print("Consumer started. Waiting for dataset from producer...")
    
    # Read the entire dataset from Kafka
    message = next(consumer)
    dataset = json.loads(message.value.decode('utf-8'))
    print("Dataset received from producer.")

    # Additional print statements for debugging
    # print("Dataset:", dataset)

    # Extract transactions from the dataset
    transactions = []
    for item in dataset:
        # Getting the asin and related items for each transaction
        transaction = [item["asin"]] + item.get("related", [])
        transactions.append(transaction)
    print("Transactions extracted from the dataset.")

    # Create a SlidingApriori instance
    sliding_window = SlidingApriori(size=len(transactions))
    print("Sliding window initialized.")

    # Add transactions to the sliding window
    for transaction in transactions:
        sliding_window.add_transaction(transaction)
    print("Transactions added to sliding window.")

    min_supp = 2
    
    # Generate frequent itemsets
    frequent_itemsets = sliding_window.generate_itemsets(transactions, min_supp)
    print("Frequent itemsets generated.")

    # Save frequent itemsets to a file
    with open(output_path, "w") as file:
        file.write("Itemset\tSupport\n")
        for itemset, support in frequent_itemsets.items():
            file.write(f"{itemset}\t{support}\n")

    print("Frequent itemsets saved to", output_path)


# Kafka settings
bootstrap_servers = 'localhost:9092'
topic = 'Apriori'

# Create Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# Output path for saving frequent itemsets
output_path = 'output/apriori_frequent_itemsets.txt'

# Clear output file when consumer starts
with open(output_path, "w") as file:
    file.write("")

# Consume dataset and generate frequent itemsets
while True:
    consume_dataset(consumer, output_path)