from kafka import KafkaProducer
import json
import time

def produce_dataset(file_path, producer, topic):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            producer.send(topic, json.dumps(data).encode('utf-8'))
            print("Dataset sent successfully")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Kafka settings
bootstrap_servers = 'localhost:9092'
apriori_topic = 'PCY_topic'
topics = ['Apriori', 'PCY', 'Custom']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Path to the dataset file
dataset_file = 'preprocessed.json'

# Infinite loop to continuously send the dataset
while True:
    for topic in topics:
        produce_dataset(dataset_file, producer, topics)
    produce_dataset(dataset_file, producer, apriori_topic)
    time.sleep(5)  # Adjust the sleep duration as needed

# Close the producer when done
producer.close()