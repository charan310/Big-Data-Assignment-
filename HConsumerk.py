from confluent_kafka import Consumer, KafkaError
from confluent_kafka.error import KafkaException

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # start consuming from the earliest message
}

# Create Kafka consumer
consumer = Consumer(consumer_conf)

# Subscribe to topic(s)
consumer.subscribe(['new_topic'])

# Poll for new messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll with a timeout of 1 second
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, the consumer reached the end of the log
                print('%% {} [{}] reached end at offset {}'.format(msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Message was successfully consumed
            print('Received message: {}'.format(msg.value().decode('utf-8')))

except KeyboardInterrupt:
    # Close consumer on interrupt
    consumer.close()
