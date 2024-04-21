from kafka import KafkaConsumer
import json
from collections import Counter
from itertools import combinations

def hash_bucket(itemset, num_buckets):
    return hash(itemset) % num_buckets

def add_transaction(transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts):
    window.append(transaction)
    for item in transaction:
        if isinstance(item, str):  # Ensure item is a string
            single_counts[item] += 1
        else:
            print(f"Ignoring invalid item: {item}")
    for pair in combinations(transaction, 2):
        bucket = hash_bucket(pair, num_buckets)
        hash_table[bucket] += 1
        if hash_table[bucket] >= hash_support:
            pair_counts[pair] += 1
    for triplet in combinations(transaction, 3):
        if all(pair in pair_counts for pair in combinations(triplet, 2)):
            triplet_counts[triplet] += 1


def remove_transaction(transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts):
    window.remove(transaction)
    for item in transaction:
        single_counts[item] -= 1
    for pair in combinations(transaction, 2):
        bucket = hash_bucket(pair, num_buckets)
        hash_table[bucket] -= 1
        if hash_table[bucket] >= hash_support:
            pair_counts[pair] -= 1
        for triplet in combinations(transaction, 3):
            triplet_counts[triplet] -= 1

def update_window(window, window_size, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts):
    if len(window) > window_size:
        old_transaction = window.pop(0)
        remove_transaction(old_transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts)
        # Remove triplets containing items from the removed transaction
        for triplet in combinations(old_transaction, 3):
            triplet_counts[triplet] -= 1

def pcy_sliding_window(consumer, num_buckets, hash_support, min_support, window_size):
    single_counts = Counter()
    pair_counts = Counter()
    triplet_counts = Counter()
    hash_table = [0] * num_buckets
    window = []

    print("PCY Sliding Window Consumer started.")

    for message in consumer:
        print("Received message from Kafka topic.")
        dataset = json.loads(message.value.decode('utf-8'))
        transactions = []
        for item in dataset:
            # Getting the asin and related items for each transaction
            transaction = [item["asin"]] + item.get("related", [])
            transactions.append(transaction)
        print("Transactions extracted from the dataset.")

        for transaction in transactions:
            add_transaction(transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts)
            update_window(window, window_size, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts)

        frequent_singles = {item for item, count in single_counts.items() if count >= min_support}
        frequent_pairs = {pair for pair, count in pair_counts.items() if count >= min_support}
        frequent_triplets = {triplet for triplet, count in triplet_counts.items() if count >= min_support}

        print("PCY Sliding Window algorithm completed.")

        return frequent_singles, frequent_pairs, frequent_triplets

# Kafka settings
bootstrap_servers = 'localhost:9092'
pcy_topic = 'PCY'

# Create Kafka consumer
consumer = KafkaConsumer(pcy_topic, bootstrap_servers=bootstrap_servers)

# Parameters
num_buckets = 10
hash_support = 2
min_support = 3
window_size = 100

# Execute PCY sliding window algorithm
frequent_singles, frequent_pairs, frequent_triplets = pcy_sliding_window(consumer, num_buckets, hash_support, min_support, window_size)

# Output results in a txt file
output_file = 'output/pcy_frequent_itemsets.txt'
with open(output_file, 'w') as file:
    file.write("Frequent Singles:\n")
    for item in frequent_singles:
        file.write(f"{item}\n")

    file.write("\nFrequent Pairs:\n")
    for pair in frequent_pairs:
        file.write(f"{pair}\n")

    file.write("\nFrequent Triplets:\n")
    for triplet in frequent_triplets:
        file.write(f"{triplet}\n")

print("Results saved to", output_file)

# Close the consumer when done
consumer.close()