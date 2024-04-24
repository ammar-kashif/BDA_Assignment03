# Streaming Data Insights: Frequent Itemset Analysis on Amazon

## Metadata

### Dataset Description
The **Amazon Metadata** dataset is a collection of product information stored in JSON format. It includes various attributes such as product ID, title, features, description, price, image URLs, related products, sales rank, brand, categories, and technical details.

### Dataset Download
You can download the Amazon Metadata dataset from [here](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/#complete-data).

## Dependencies
- **Python Libraries:**
  - [Kafka-Python](https://github.com/dpkp/kafka-python): `pip install kafka-python`
  - [Pandas](https://pandas.pydata.org/): `pip install pandas`
  - [TQDM](https://github.com/tqdm/tqdm): `pip install tqdm`
  - [JSON](https://docs.python.org/3/library/json.html): Already included in Python.
  - [RE](https://docs.python.org/3/library/re.html): Already included in Python.
  - [Itertools](https://docs.python.org/3/library/itertools.html): Already included in Python.
  
- **Softwares:**
  - [Apache Kafka](https://kafka.apache.org/): Download and setup instructions [here](https://kafka.apache.org/downloads).
  - [Python](https://www.python.org/): Download and installation guide [here](https://www.python.org/downloads/).

## Features
- Sampling and Preprocessing the Dataset
- Setting up Streaming Pipeline
- Implementing Frequent Itemset Mining Algorithms
- Integrating with Database
- Bash Script for Enhanced Project Execution

## How to Use
1. **Sampling and Preprocessing:**
   - Download the Amazon Metadata dataset.
   - Execute `preprocess.ipynb` to sample and preprocess the dataset.

2. **Streaming Pipeline Setup:**
   - Develop a producer application (`producer.py`) to stream preprocessed data.
   - Create consumer applications (`apriori_consumer.py`, `pcy_consumer.py`, `custom_consumer.py`) to subscribe to the producer's data stream.

3. **Frequent Itemset Mining:**
   - Implement the Apriori algorithm in `apriori_consumer.py`.
   - Implement the PCY algorithm in `pcy_consumer.py`.
   - Implement custom analysis in `custom_consumer.py`.

4. **Database Integration:**
   - Connect each consumer to a database and store the results.

5. **Bash Script:**
   - Utilize the provided bash script to initialize Kafka components and run the producer and consumers seamlessly.

## Why Choose Our Solution
- Efficient preprocessing techniques to handle large datasets.
- Real-time streaming pipeline for immediate insights.
- Implementation of popular frequent itemset mining algorithms.
- Flexible database integration for data persistence.
- Bash script automates project execution, enhancing usability.

## Usage
1. Clone the repository.
2. Download the Amazon Metadata dataset.
3. Execute the preprocessing script to sample and preprocess the dataset.
4. Run the provided bash script to initialize Kafka components and execute the producer and consumers.
5. Analyze the generated frequent itemsets and association rules.

## Team:
Meet the dedicated individuals who contributed to this project:

- **Ammar Khasif**: [GitHub](https://github.com/ammar-kashif).
- **Arhum Khan**: [GitHub](https://github.com/Arhum-Khan10).
- **Aaqib Ahmed Nazir**: [GitHub](https://github.com/aaqib-ahmed-nazir).
