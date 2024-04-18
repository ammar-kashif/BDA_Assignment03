from itertools import combinations
import json

def create_transactions(data):
    transactions = []
    for item in data:
        transaction = [item['asin']] + item.get('related', [])
        transactions.append(transaction)
    return transactions

def find_frequent_pairs(transactions, min_support):
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
    return {pair: count for pair, count in pair_counts.items() if count >= min_support}

def generate_association_rules(frequent_pairs, transactions, min_confidence):
    total_transactions = len(transactions)
    rules = set()  # Using a set to store rules to avoid duplicates
    # Compute the support for each pair directly from frequent_pairs
    item_support = {item: count / total_transactions for item, count in frequent_pairs.items()}

    for pair, pair_support in item_support.items():
        items = list(pair)
        item1, item2 = sorted(items)  # Sort items to ensure consistent order
        # Support of individual items
        support1 = sum(1 for t in transactions if item1 in t) / total_transactions
        support2 = sum(1 for t in transactions if item2 in t) / total_transactions

        # Confidence calculations
        confidence1to2 = pair_support / support1
        confidence2to1 = pair_support / support2

        # Only add rule if confidence is high enough and not already added
        if confidence1to2 >= min_confidence:
            rules.add((item1, item2, confidence1to2))  # Add sorted rule
        if confidence2to1 >= min_confidence and (item2, item1) not in rules:
            rules.add((item2, item1, confidence2to1))  # Check if reverse isn't already added

    return rules

# Example usage with the rest of the code remains the same


with open('preprocessed_dataset.json', 'r') as file:
    json_data = json.load(file)
    # reading the first 5 records
    # json_data = json_data[:100]
    

transactions = create_transactions(json_data)
min_support = 2
frequent_pairs = find_frequent_pairs(transactions, min_support)

min_confidence = 0.5
rules = generate_association_rules(frequent_pairs, transactions, min_confidence)

# Output results in a txt file
output_file = 'frequent_itemsets.txt'
with open(output_file, 'w') as file:
    # file.write("Frequent Itemsets:\n")
    # for pair, support in frequent_pairs.items():
    #     file.write(f"{pair}: {support}\n")

    file.write("\nAssociation Rules:\n")
    for antecedent, consequent, confidence in rules:
        file.write(f"Rule: {antecedent} -> {consequent} (Confidence: {confidence:.2f})\n")

