import json
from collections import defaultdict, deque
from itertools import combinations, permutations

# Define the SlidingWindow class
class AprioriSlidingWindow:
    def __init__(self, size=100):
        self.window = deque()
        self.size = size
        self.itemsets = defaultdict(int)

    def add_transaction(self, transaction):
        if len(self.window) >= self.size:
            self.remove_transaction()
        self.window.append(transaction)
        self.update_counts(transaction, 1)

    def remove_transaction(self):
        old_transaction = self.window.popleft()
        self.update_counts(old_transaction, -1)

    def update_counts(self, transaction, increment):
        max_length = 4  # Adjust this value based on the expected complexity
        for r in range(1, min(max_length + 1, len(transaction) + 1)):
            for itemset in combinations(transaction, r):
                self.itemsets[itemset] += increment
                if self.itemsets[itemset] <= 0:
                    del self.itemsets[itemset]

    def generate_itemsets(self ,transactions, min_support, max_length = 3):
        itemset_counts = defaultdict(int)

        # Count occurrences of each itemset
        for transaction in transactions:
            for r in range(1, max_length + 1):
                for itemset in combinations(transaction, r):
                    itemset_counts[itemset] += 1

        # Filter itemsets based on minimum support
        frequent_itemsets = {itemset: count for itemset, count in itemset_counts.items() if count >= min_support}
        return frequent_itemsets

        


def read_transactions_from_json(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    transactions = []
    for item in data:
        transaction = [item['asin']] + item.get('related', [])
        transactions.append(transaction)
    return transactions


    
def generate_association_rules(frequent_pairs, transactions, min_confidence):
    total_transactions = len(transactions)
    rules = set()  # Using a set to store rules to avoid duplicates
    # Compute the support for each pair directly from frequent_pairs
    item_support = {item: count / total_transactions for item, count in frequent_pairs.items()}

    for pair, pair_support in item_support.items():
        items = list(pair)
        for i in range(1, len(items)):
            for antecedent in permutations(items, i):  # Generate all permutations of items
                antecedent = tuple(sorted(antecedent))
                consequent = tuple(sorted(set(items) - set(antecedent)))
                antecedent_support = frequent_pairs.get(antecedent, 0) / total_transactions
                if antecedent_support > 0:
                    confidence = pair_support / antecedent_support
                    if confidence >= min_confidence:
                        rules.add((antecedent, consequent, confidence))

    return rules

# Calculate frequent itemsets
data_path = "preprocessed_dataset.json"
output_path = "output.txt"

# Read transactions
transactions = read_transactions_from_json(data_path)

if not transactions:
    print("No transactions loaded.")
else:
    sliding_window = AprioriSlidingWindow(size=100)
    for transaction in transactions:
        sliding_window.add_transaction(transaction)

    min_support = 2
    frequent_itemsets = sliding_window.generate_itemsets(transactions, min_support)
    print(f"Frequent Itemsets: {frequent_itemsets}")

    # save results to a file
    with open(output_path, "w") as file:
        file.write("Itemset\tSupport\n")
        for itemset, support in frequent_itemsets.items():
            file.write(f"{itemset}\t{support}\n")

    # Minimum confidence for association rules
    min_confidence = 0.5
    
    # Generate association rules
    association_rules = generate_association_rules(frequent_itemsets, transactions, min_confidence)        
    # save results to a file
    with open("association_rules.txt", "w") as file:
        file.write("Antecedent\tConsequent\tConfidence\n")
        for antecedent, consequent, confidence in association_rules:
            file.write(f"{antecedent}\t{consequent}\t{confidence:.2f}\n")
            
    print(f"Results saved to {output_path} and association_rules.txt")