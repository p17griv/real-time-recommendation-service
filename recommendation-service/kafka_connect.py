from kafka import KafkaProducer, KafkaConsumer


# Establish connection with the kafka broker as producer
def connect_kafka_producer():
    try:
        return KafkaProducer(bootstrap_servers=['192.168.159.131:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Kafka Producer: Exception while connecting to Kafka')
        print(str(ex))


# Establish connection with the kafka broker as consumer on a topic
def connect_kafka_consumer(topic, start_from='latest'):
    try:
        return KafkaConsumer(topic, auto_offset_reset=start_from,
                             bootstrap_servers=['192.168.159.131:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    except Exception as ex:
        print('Kafka Consumer: Exception while connecting to Kafka')
        print(str(ex))
