from kafka import KafkaProducer
import json
import ijson
import time

def produce_dataset(file_path, producer, topics, batch_size=20):
    try:
        with open(file_path, "r") as file:
            # read the JSON file in a streaming manner
            objects = ijson.items(file, 'item')
            batch = []

            # loop through the objects and send them in batches
            for obj in objects:
                batch.append(obj)
                if len(batch) >= batch_size:
                    # send the batch to all topics
                    for topic in topics:
                        producer.send(topic, json.dumps(batch).encode('utf-8'))
                        producer.flush()
                    batch = []
                    print(f"Sent {batch_size} objects to {topics}")
                    time.sleep(5)

            # send the last partial batch if there is any
            if batch:
                for topic in topics:
                    producer.send(topic, json.dumps(batch).encode('utf-8'))
                    producer.flush()

    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# declare parameters
bootstrap_servers = 'localhost:9092'
dataset_file = 'data/preprocessed_dataset.json'
topics = ['Apriori', 'PCY', 'Custom']

# intialize the producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# start producing the dataset
produce_dataset(dataset_file, producer, topics)

producer.close()