from kafka import KafkaProducer
import json
import ijson
import time

def produce_dataset(file_path, producer, topics, batch_size=20):
    try:
        with open(file_path, "r") as file:
            objects = ijson.items(file, 'item')  # Assuming the JSON array is the root element
            batch = []
            for obj in objects:
                batch.append(obj)
                if len(batch) >= batch_size:
                    for topic in topics:
                        producer.send(topic, json.dumps(batch).encode('utf-8'))
                        producer.flush()
                    batch = []
                    print(f"Sent {batch_size} objects to {topics}")
                    time.sleep(2)
            if batch:  # Send the last partial batch if there is any
                for topic in topics:
                    producer.send(topic, json.dumps(batch).encode('utf-8'))
                    producer.flush()
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Kafka settings
bootstrap_servers = 'localhost:9092'
# apriori_topic = 'PCY_topic'
topics = ['Apriori', 'PCY', 'Custom']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Path to the dataset file
dataset_file = 'data/preprocessed_dataset.json'

# Infinite loop to continuously send the dataset
while True:
    produce_dataset(dataset_file, producer, topics)

    time.sleep(1)  # Adjust the sleep duration as needed