from kafka import KafkaConsumer
import json
import os
import time

# --- Configuration ---
# IMPORTANT: This script runs LOCALLY on your macOS.
# Based on your 'docker compose ps' output, Kafka is accessible on kafka:9092.
# For LOCAL execution, 'kafka:9092' is usually the correct default.
# 'kafka:9092' would be used if this consumer itself was running inside a Docker container
# and connecting to the 'kafka' service within the same Docker network.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092') # <--- SUGGESTED: Default to 'kafka:9092' for local execution
print(f"Consumer attempting to connect to KAFKA_BROKER: {KAFKA_BROKER}") # ADD THIS LINE
TOPIC_NAME = 'IBM_HR_data_topic' # Corrected topic name to match producer
GROUP_ID = 'IBM_HR_group_new_test'

def deserialize_data(data_bytes):
    """Deserialize JSON bytes from Kafka message."""
    # Decode the bytes received from Kafka back into a UTF-8 string.
    decoded_string = data_bytes.decode('utf-8')
    # Parse the JSON string into a Python dictionary.
    return json.loads(decoded_string)


def consume_messages():
    consumer = None
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest', # Start reading from the beginning of the topic (useful for testing)
            enable_auto_commit=True,      # Automatically commit offsets
            group_id=GROUP_ID, # Assign a consumer group ID
            value_deserializer=deserialize_data,
            # request_timeout_ms=30000, # Uncomment if you face connection timeouts
            # api_version=(0, 10, 1) # Uncomment if you encounter API version mismatch errors
        )
        print(f"Consumer connected to Kafka broker: {KAFKA_BROKER}")
        print(f"Subscribed to topic: '{TOPIC_NAME}' in group '{GROUP_ID}'. Waiting for messages... (Press Ctrl+C to stop)")


        # Process messages
        for message in consumer:
            employee_record = message.value # 'message.value' is now a Python dictionary after deserialization
            print(f"--- Received Employee Record (Offset: {message.offset}, Partition: {message.partition}) ---")
            # Print specific fields for better readability of structured data
            print(f"  BusinessTravel: {employee_record.get('BusinessTravel', 'N/A')}")
            print(f"  Attrition: {employee_record.get('Attrition', 'N/A')}")
            print(f"  Department: {employee_record.get('Department', 'N/A')}")
            print(f"  EducationField: {employee_record.get('EducationField', 'N/A')}")
            print(f"  Gender: {employee_record.get('Gender', 'N/A')} (Type: {type(employee_record.get('Gender'))})")
            print(f"  JobRole: {employee_record.get('JobRole', 'N/A')} (Type: {type(employee_record.get('JobRole'))})")
            print(f"  MaritalStatus: {employee_record.get('MaritalStatus', 'N/A')}")
            print(f"  Over18: {employee_record.get('Over18', 'N/A')}")
            print(f"  OverTime: {employee_record.get('OverTime', 'N/A')}")
            print("-" * 40)
            time.sleep(0.05) # Small delay to simulate processing


    except KeyboardInterrupt:
        print("\nConsumer gracefully stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
        print("Please ensure your Kafka container is running, the topic exists, and the broker address is correct.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer connection closed.")


if __name__ == "__main__":
    consume_messages()
