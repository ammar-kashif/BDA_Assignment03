from kafka import KafkaProducer
import json
import time

def produce_dataset(file_path, producer, topics, num_objects=10):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            total_objects = len(data)
            for i in range(0, total_objects, num_objects):
                # Send chunks of num_objects each time
                chunk = data[i:i+num_objects]
                chunk_data = json.dumps(chunk).encode('utf-8')
                for topic in topics:
                    producer.send(topic, chunk_data)
                    producer.flush()  # Ensure data is sent immediately
                    print(f"Sent {num_objects} objects to {topic}")
                time.sleep(1)  # Sleep to simulate time delay between sends
                if i + num_objects >= total_objects:
                    print("End of dataset reached.")
                    break
            producer.send(topic, json.dumps(data).encode('utf-8'))
            print("Dataset sent successfully")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Kafka settings
bootstrap_servers = 'localhost:9092'
# apriori_topic = 'PCY_topic'
topics = ['Apriori', 'PCY']

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Path to the dataset file
dataset_file = 'preprocessed_dataset.json'

# Infinite loop to continuously send the dataset
while True:
    produce_dataset(dataset_file, producer, topics)

    time.sleep(2)  # Adjust the sleep duration as needed