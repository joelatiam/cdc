import os
import re
import random
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

# Kafka broker and regular expression pattern for topics
kafka_config = {
    'bootstrap.servers': os.environ.get('KAFKA_BROKER'),
    'group.id': os.environ.get('KAFKA_GROUP_ID'),
    'auto.offset.reset': os.environ.get('KAFKA_MESSAGE_OFFSET')
}

topic_pattern = re.compile(os.environ.get('KAFKA_TOPIC_PATTERN'))

# Kafka consumer
consumer = Consumer(kafka_config)

# Get a list of all topics and filter by the regular expression
metadata = consumer.list_topics().topics
matching_topics = [topic for topic in metadata if topic_pattern.match(topic)]

# Randomly select a topic from the matching topics
if matching_topics:
    random_topic = random.choice(matching_topics)
    consumer.subscribe([random_topic])
    print(f'Subscribed to randomly selected topic: {random_topic}')
else:
    print('No topics matched the regular expression.')

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while consuming message: {}'.format(msg.error()))
    else:
        print('Received message: {}'.format(msg.value().decode('utf-8')))
