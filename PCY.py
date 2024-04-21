import json
from itertools import combinations
from collections import Counter, defaultdict

def create_transactions_generator(data):
    for item in data:
        yield [item['asin']] + item.get('related', [])

def hash_bucket(itemset, num_buckets):
    return hash(itemset) % num_buckets

def pcy_algorithm(transactions_generator, num_buckets, hash_support, min_support, window_size):
    pair_counts = Counter()
    triplet_counts = Counter()
    single_counts = Counter()
    hash_table = [0] * num_buckets
    window = []

    for transaction in transactions_generator:
        window.append(transaction)
        single_counts.update(transaction)
        if len(window) > window_size:
            old_transaction = window.pop(0)
            for pair in combinations(old_transaction, 2):
                bucket = hash_bucket(pair, num_buckets)
                hash_table[bucket] -= 1
                if hash_table[bucket] >= hash_support:
                    pair_counts[pair] -= 1
                for triplet in combinations(pair, 3):
                    triplet_counts[triplet] -= 1

        for pair in combinations(transaction, 2):
            bucket = hash_bucket(pair, num_buckets)
            hash_table[bucket] += 1
            if hash_table[bucket] >= hash_support:
                pair_counts[pair] += 1
            for triplet in combinations(pair, 3):
                triplet_counts[triplet] += 1

    frequent_items = {item for item, count in single_counts.items() if count >= min_support}
    frequent_pairs = {pair for pair, count in pair_counts.items() if count >= min_support}
    frequent_triplets = {triplet for triplet, count in triplet_counts.items() if count >= min_support}

    return frequent_items, frequent_pairs, frequent_triplets


with open('first_100_rows.json', 'r') as file:
    json_data = json.load(file)
    # reading the first 5 records
    # json_data = json_data[:100]
    
# Parameters
num_buckets = 10
hash_support = 2
min_support = 3
window_size = 100
transactions_generator = create_transactions_generator(json_data)
frequent_items, frequent_pairs, frequent_triplets = pcy_algorithm(transactions_generator, num_buckets, hash_support, min_support, window_size)

# Output results in a txt file
output_file = 'PCY_frequent_itemsets.txt'
with open(output_file, 'w') as file:
    file.write("Frequent Single Items:\n")
    for item in frequent_items:
        file.write(f"{item}\n")
    
    file.write("\nFrequent Pairs:\n")
    for pair in frequent_pairs:
        file.write(f"{pair}\n")

    file.write("\nFrequent Triplets:\n")
    for triplet in frequent_triplets:
        file.write(f"{triplet}\n")

print("Results saved to", output_file)
