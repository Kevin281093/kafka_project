from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Source Configuration (Nguồn)
SOURCE_BROKERS = "113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294"
SOURCE_TOPIC = "product_view"  # Source topic
SOURCE_GROUP_ID = "source_consumer_group"
SOURCE_USERNAME = "kafka"
SOURCE_PASSWORD = "UnigapKafka@2024"

# Kafka Target Configuration (Đích)
TARGET_BROKERS = "localhost:9094,localhost:9194,localhost:9294 "  # Kafka local
TARGET_TOPIC = "project_topic"  # Target topic
TARGET_USERNAME = "admin"
TARGET_PASSWORD = "Unigap@2024"

# MongoDB Configuration
MONGO_URI = "mongodb+srv://kevinpx93:281093@cluster0.g5urw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB = "kafka"
MONGO_COLLECTION = "project"

# Function to create Kafka Producer
def create_producer(brokers, username=None, password=None):
    producer_config = {
        "bootstrap_servers": brokers.split(","),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
    }

    if username and password:
        producer_config.update({
            "security_protocol": "SASL_PLAINTEXT",  # Hoặc SASL_SSL nếu cần bảo mật hơn
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": username,
            "sasl_plain_password": password,
        })

    return KafkaProducer(**producer_config)

# Function to create Kafka Consumer
def create_consumer(brokers, group_id, topic, username=None, password=None):
    consumer_config = {
        "bootstrap_servers": brokers.split(","),
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "value_deserializer": lambda v: v.decode("utf-8"),
        "key_deserializer": lambda k: k.decode("utf-8") if k else None,
    }

    if username and password:
        consumer_config.update({
            "security_protocol": "SASL_PLAINTEXT",  # Hoặc SASL_SSL nếu cần bảo mật hơn
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": username,
            "sasl_plain_password": password,
        })

    return KafkaConsumer(topic, **consumer_config)


# Step 1: Read from Kafka Source and Produce to Kafka Target
# def transfer_kafka_source_to_target():
#     consumer = create_consumer(SOURCE_BROKERS, SOURCE_GROUP_ID, SOURCE_TOPIC, username=SOURCE_USERNAME, password=SOURCE_PASSWORD)
#     producer = create_producer(TARGET_BROKERS, username=TARGET_USERNAME, password=TARGET_PASSWORD)
#
#     try:
#         for msg in consumer:
#             data = msg.value
#             print(f"Received from source: {data}")
#
#             # Produce to Target Kafka
#             producer.send(TARGET_TOPIC, key=msg.key, value=data)
#             print(f"Produced to target: {data}")
#             producer.flush()
#     except Exception as e:
#         print(f"Error during Kafka transfer: {e}")
#     finally:
#         consumer.close()

# Step 2: Consume from Kafka Target and Store to MongoDB
def consume_kafka_target_and_store_to_mongo():
    print("🔄 Connecting to Kafka...")
    consumer = create_consumer(TARGET_BROKERS, None, TARGET_TOPIC, username=TARGET_USERNAME, password=TARGET_PASSWORD)
    print("✅ Connected to Kafka! Waiting for messages...")
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    print("✅ Connected to Mongo! Waiting for messages...")
    try:
        print("consume")
        for msg in consumer:
            print(f"📩 Received message: {msg}")
            data = None
            try:
                print(f"📩 Raw Kafka message: {msg.value}")
                data = json.loads(msg.value)
                print(f"Consumed from target: {data}")
            except json.JSONDecodeError as e:
                print("Error parse JSON:", e)

            # Insert into MongoDB
            if data is not None:
                try:
                    collection.insert_one(data)
                    print(f"Inserted into MongoDB: {data}")
                except Exception as db_error:
                    print(f"MongoDB Insert Error: {db_error}")
    except Exception as e:
        print(f"Error during MongoDB storage: {e}")
    finally:
        print("debug")
        consumer.close()

# Main Function
if __name__ == "__main__":
    # print("Step 1: Transfer data from Kafka source to Kafka target...")
    # transfer_kafka_source_to_target()

    print("Step 2: Consume data from Kafka target and store to MongoDB...")
    consume_kafka_target_and_store_to_mongo()