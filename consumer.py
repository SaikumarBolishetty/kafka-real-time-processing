from confluent_kafka import Consumer, KafkaError, Producer
import json
import time

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)

# Kafka Producer Configuration to send processed messages
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
}

producer = Producer(producer_conf)

# Subscribe to 'user-login' topic
consumer.subscribe(['user-login'])

# Initialize aggregation counters for device types and locales
device_type_counts = {}
locale_counts = {}

def process_message(message):
    """Process the consumed message by transforming, filtering, and aggregating"""
    user_data = json.loads(message)
    
    # Filtering: Skip the message if 'device_type' or 'app_version' is missing or locale is 'RU'
    if 'device_type' not in user_data or 'app_version' not in user_data or user_data['locale'] == 'RU':
        return None

    # Transformation: Add processed timestamp
    user_data['processed_timestamp'] = int(time.time())

    # Aggregation: Count logins by device type and locale
    device_type = user_data['device_type']
    locale = user_data['locale']
    
    device_type_counts[device_type] = device_type_counts.get(device_type, 0) + 1
    locale_counts[locale] = locale_counts.get(locale, 0) + 1

    return json.dumps(user_data)

def send_aggregated_data():
    """Periodically send aggregated data to Kafka"""
    aggregation_data = {
        'device_type_counts': device_type_counts,
        'locale_counts': locale_counts,
        'aggregation_timestamp': int(time.time())
    }
    producer.produce('processed-user-login', json.dumps(aggregation_data), callback=delivery_report)
    producer.flush()

def delivery_report(err, msg):
    """Callback to report message delivery status"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Consume messages from 'user-login' topic
try:
    print("Consuming messages from 'user-login'...")

    # Track number of messages processed
    message_count = 0  
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the message
        processed_data = process_message(msg.value().decode('utf-8'))

        # If the message was successfully processed
        if processed_data:
            # Produce the processed message to the new topic 'processed-user-login'
            producer.produce('processed-user-login', processed_data, callback=delivery_report)
            producer.flush()
        
        # Aggregation step, Send aggregation every 100 messages
        message_count += 1
        if message_count % 100 == 0:
            send_aggregated_data()

except KeyboardInterrupt:
    print("Consumer interrupted.")

finally:
    consumer.close()