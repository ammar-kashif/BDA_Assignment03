import pandas as pd
import itertools


def apriori(transactions, min_support):
    item_counts = {}
    for transaction in transactions:
        for item in transaction:
            if item in item_counts:
                item_counts[item] += 1
            else:
                item_counts[item] = 1

    # Applying minimum support to filter items
    frequent_itemsets = {frozenset([item]): count for item, count in item_counts.items() if count >= min_support}
    k = 2
    while True:
        candidate_sets = generate_candidates(frequent_itemsets, k)
        if not candidate_sets:
            break
        frequent_itemsets.update({itemset: 0 for itemset in candidate_sets})
        for transaction in transactions:
            for candidate in candidate_sets:
                if candidate.issubset(transaction):
                    frequent_itemsets[candidate] += 1
        frequent_itemsets = {itemset: count for itemset, count in frequent_itemsets.items() if count >= min_support}
        k += 1

    return frequent_itemsets


def generate_association_rules(frequent_itemsets, transactions, min_confidence):
    rules = []
    for itemset in frequent_itemsets:
        if len(itemset) > 1:
            for consequent in itemset:
                antecedent = itemset - {consequent}
                antecedent_support = sum(1 for transaction in transactions if antecedent.issubset(transaction))
                confidence = frequent_itemsets[itemset] / antecedent_support
                if confidence >= min_confidence:
                    rules.append((antecedent, [consequent], confidence))
    return rules


def generate_candidates(frequent_itemsets, k):
    candidates = set()
    for itemset1, itemset2 in itertools.combinations(frequent_itemsets, 2):
        union_set = itemset1.union(itemset2)
        if len(union_set) == k:
            candidates.add(union_set)
    return candidates

# Loading the data
Amazon_data = pd.read_json('preprocessed_dataset.json')
transactions = Amazon_data['related'].values

# Find frequent itemsets with a minimum support threshold using Apriori algorithm
min_support = 2  
frequent_itemsets = apriori(transactions, min_support)

# Displaying the frequent itemsets
print("Frequent Itemsets:\n")
for i, (itemset, support) in enumerate(frequent_itemsets.items(), start=1):
    itemset_str = ', '.join(itemset)
    print(f"Itemset {i}:")
    print(f"  Items:        {itemset_str}")
    print(f"  Support:      {support}")
    print()
    # Writing to file
    with open('frequent_itemsets.txt', 'a') as f:
        f.write(f"Itemset {i}:\n")
        f.write(f"  Items:        {itemset_str}\n")
        f.write(f"  Support:      {support}\n\n")

# Generating association rules from the frequent itemsets
# min_confidence = 0.5  
# rules = generate_association_rules(frequent_itemsets, transactions, min_confidence)

# Displaying the association rules
# print("Association Rules:\n")
# for i, (antecedent, consequent, confidence) in enumerate(rules, start=1):
#     antecedent_str = ', '.join(antecedent)
#     consequent_str = ', '.join(consequent)
#     print(f"Rule {i}:")
#     print(f"  Antecedent:   {antecedent_str}")
#     print(f"  Consequent:   {consequent_str}")
#     print(f"  Confidence:   {confidence:.2f}")
#     print()
