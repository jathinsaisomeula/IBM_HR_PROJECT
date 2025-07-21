import csv
from kafka import KafkaProducer
import json
import time
import os
import uuid

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'IBM_HR_data_topic' # Changed to a more descriptive topic name
CSV_FILE = 'IBM_HR_data.csv' # Changed to match your uploaded file name

def serialize_data(row): # Renamed function parameter from 'data' to 'row' for clarity
    """
    Serializes dictionary data from CSV into JSON bytes.
    Adds a unique ID and performs type conversions for specific columns
    relevant to the IBM HR DATA dataset.
    """
    # Initialize a new dictionary to build the processed record
    processed_row = {}

    # Add a unique ID if not present or empty
    # Check 'id' in the input 'row', not an undefined 'data' variable
    if 'id' not in row or not row['id']:
        processed_row['id'] = str(uuid.uuid4())
    else:
        processed_row['id'] = row['id'] # Keep existing ID if it exists

    for key, value in row.items(): # Iterate over items in 'row', not 'data'
        # Handle empty strings or None values uniformly
        if value is None or str(value).strip() == '':
            processed_row[key] = None
        # Numerical Columns (mostly integers) - Using exact column names from your CSV header
        elif key in [
            "Age",
            "DailyRate",
            "DistanceFromHome",
            "Education", # Education is numerical (level)
            "EmployeeCount",
            "EmployeeNumber",
            "EnvironmentSatisfaction",
            "HourlyRate",
            "JobInvolvement",
            "JobLevel",
            "JobSatisfaction",
            "MonthlyIncome",
            "MonthlyRate",
            "NumCompaniesWorked",
            "PercentSalaryHike",
            "PerformanceRating",
            "RelationshipSatisfaction",
            "StandardHours",
            "StockOptionLevel",
            "TotalWorkingYears",
            "TrainingTimesLastYear",
            "WorkLifeBalance",
            "YearsAtCompany",
            "YearsInCurrentRole",
            "YearsSinceLastPromotion",
            "YearsWithCurrManager"
        ]:
            try:
                # Attempt to convert to int (via float for robustness against "10.0")
                processed_row[key] = int(float(value))
            except (ValueError, TypeError):
                processed_row[key] = None # Set to None if conversion fails
        # All other columns (categorical/strings) - keep as strings (strip whitespace)
        else:
            processed_row[key] = str(value).strip() # Ensure all remaining are strings and strip whitespace

    return json.dumps(processed_row).encode('utf-8') # Dump the processed_row


def produce_messages():
    producer = None
    try:
        producer = KafkaProducer(
            # bootstrap_servers expects a list of brokers, not just a string
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=serialize_data # Corrected function name to serialize_data
        )

        print(f"Producer connected to KAFKA_BROKER: {KAFKA_BROKER}") # Corrected print message capitalization

        # The 'with open' block must be inside the 'try' block to catch FileNotFoundError
        with open(CSV_FILE, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            print(f"Reading data from '{CSV_FILE}' and sending to topic '{TOPIC_NAME}'...")
            message_count = 0
            for row in csv_reader:
                # producer.send(TOPIC_NAME, row) # 'row' will be processed by serialize_data
                # Pass a copy of the row if you intend to modify it in serialize_data AND use the original in logging
                # Or just use the original 'row' here and ensure serialize_data modifies it in place or returns a new dict
                # Given serialize_data creates `processed_row`, passing `row` is fine.
                producer.send(TOPIC_NAME, row)

                # Adjusted logging message to use relevant IBM HR data fields
                print(f"Sent message (ID: {row.get('id', 'N/A')}, EmployeeNumber: {row.get('EmployeeNumber', 'N/A')}, Department: {row.get('Department', 'N/A')})")
                message_count += 1
                time.sleep(0.1) # Small delay to simulate real-time data flow

            producer.flush()
            print(f"\nSuccessfully sent {message_count} messages to Kafka.")

    except FileNotFoundError: # Specifically catch FileNotFoundError
        print(f"Error: The file '{CSV_FILE}' was not found. Please ensure it's in the same directory.")
    except Exception as e:
        print(f"Error producing messages: {e}")
        print("Please ensure your Kafka container is running and accessible at the specified broker address.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally: # 'finally' block must align with 'try'
        if producer:
            producer.close()
            print("Producer connection closed.")


if __name__ == "__main__":
    produce_messages()
 
