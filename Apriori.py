import json
from itertools import combinations
from collections import defaultdict, deque
from itertools import combinations

class SlidingApriori:
    def __init__(self, size=100):
        self.window = deque()
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
        old_transaction = self.window.popleft()
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
        return {pair: count for pair, count in pair_counts.items() if count >= min_support}
        


def extract_transactions(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    transactions = []
    for item in data:
        # Getting the asin and related items for each transaction
        transaction = [item["asin"]] + item.get("related", [])
        transactions.append(transaction)
    return transactions


def generate_association_rules(freq_pairs, transactions, min_confidence):
    total_transactions = len(transactions)
    rules = set() 
    
    # Calculating support and confidence for each pair
    item_support = {
        item: count / total_transactions for item, count in freq_pairs.items()
    }

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


# Calculating frequent itemsets
data_path = "first_100_rows.json"
output_path = "output.txt"

# Read transactions
transactions = extract_transactions(data_path)

if not transactions:
    print("*Erorr: No transactions loaded.*")
else:
    sliding_window = SlidingApriori(size=100)
    for transaction in transactions:
        sliding_window.add_transaction(transaction)

    min_supp = 2
    
    frequent_itemsets = sliding_window.generate_itemsets(transactions, min_supp)
    print(f"Frequent Itemsets: {frequent_itemsets}")

    # saving results to a file
    with open(output_path, "w") as file:
        file.write("Itemset\tSupport\n")
        for itemset, support in frequent_itemsets.items():
            file.write(f"{itemset}\t{support}\n")

    min_confidence = 0.5

    # Getting association rules
    association_rules = generate_association_rules(
        frequent_itemsets, transactions, min_confidence
    )
    
    # Saving the rules to a file
    with open("association_rules.txt", "w") as file:
        file.write("Antecedent\tConsequent\tConfidence\n")
        for antecedent, consequent, confidence in association_rules:
            file.write(f"{antecedent}\t{consequent}\t{confidence:.2f}\n")