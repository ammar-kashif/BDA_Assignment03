from collections import Counter
from itertools import combinations
import json

def hash_bucket(itemset, num_buckets):
    return hash(itemset) % num_buckets

def create_transactions(data):
    transactions = []
    for item in data:
        transaction = [item['asin']] + item.get('related', [])
        transactions.append(transaction)
    return transactions

def pcy_algorithm(transactions, num_buckets, hash_support, min_support):
    # First pass: Count pairs in baskets
    pair_counts = Counter()
    single_counts = Counter()
    for transaction in transactions:
        single_counts.update(transaction)
        for pair in combinations(transaction, 2):
            pair_counts[pair] += 1

    # Second pass: Apply PCY algorithm
    frequent_items = {item for item, count in single_counts.items() if count >= min_support}
    frequent_pairs = {pair for pair, count in pair_counts.items() if count >= min_support}

    # Create hash table
    hash_table = [0] * num_buckets
    for pair in frequent_pairs:
        bucket = hash_bucket(pair, num_buckets)
        hash_table[bucket] += 1

    # Third pass: Count pairs in baskets, using hash table
    pair_counts = Counter()
    for transaction in transactions:
        for pair in combinations(transaction, 2):
            if all(hash_table[hash_bucket(pair, num_buckets)] >= hash_support for pair in combinations(pair, 2)):
                pair_counts[pair] += 1

    return frequent_items, frequent_pairs, pair_counts

# Example usage
with open('preprocessed_dataset.json', 'r') as file:
    data = json.load(file)

transactions = create_transactions(data)
num_buckets = 10
hash_support = 2
min_support = 2
frequent_items, frequent_pairs, pair_counts = pcy_algorithm(transactions, num_buckets, hash_support, min_support)

# Output results in a txt file
output_file = 'frequent_itemsets.txt'
with open(output_file, 'w') as file:
    # file.write("Frequent Itemsets:\n")
    # for item in frequent_items:
    #     file.write(f"{item}\n")

    # file.write("\nFrequent Pairs:\n")
    # for pair in frequent_pairs:
    #     file.write(f"{pair}\n")

    file.write("\nPair Counts:\n")
    for pair, count in pair_counts.items():
        if count >= min_support:
            file.write(f"{pair}: {count}\n")
