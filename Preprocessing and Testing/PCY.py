from collections import Counter
from itertools import combinations
import json

def hash_bucket(itemset, num_buckets):
    return hash(itemset) % num_buckets

def create_transactions_generator(data):
    for item in data:
        yield [item['asin']] + item.get('related', [])

def add_transaction(transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts):
    window.append(transaction)
    for item in transaction:
        single_counts[item] += 1
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

def pcy_sliding_window(transactions_generator, num_buckets, hash_support, min_support, window_size):
    single_counts = Counter()
    pair_counts = Counter()
    triplet_counts = Counter()
    hash_table = [0] * num_buckets
    window = []

    for transaction in transactions_generator:
        add_transaction(transaction, window, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts)
        update_window(window, window_size, hash_table, num_buckets, hash_support, single_counts, pair_counts, triplet_counts)

    frequent_singles = {item for item, count in single_counts.items() if count >= min_support}
    frequent_pairs = {pair for pair, count in pair_counts.items() if count >= min_support}
    frequent_triplets = {triplet for triplet, count in triplet_counts.items() if count >= min_support}

    return frequent_singles, frequent_pairs, frequent_triplets

with open('first_100_rows.json', 'r') as file:
    json_data = json.load(file)

transactions_generator = create_transactions_generator(json_data)

# Parameters
num_buckets = 10
hash_support = 2
min_support = 3
window_size = 100
frequent_singles, frequent_pairs, frequent_triplets = pcy_sliding_window(transactions_generator, num_buckets, hash_support, min_support, window_size)

# Output results in a txt file
output_file = 'PCY_frequent_itemsets_sliding_window.txt'
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
