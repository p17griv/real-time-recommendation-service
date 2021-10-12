import random
import json
import time
from kafka_connect import connect_kafka_producer, connect_kafka_consumer


# Sends random items to kafka topic
def publish_item(topic_name, key, number_of_items):
    producer = connect_kafka_producer()

    # Get the existing items in the topic
    existing_items = []
    consumer = connect_kafka_consumer(topic_name, 'earliest')
    for msg in consumer:
        existing_items.append(str(msg.value)[2:].replace("'", ""))

    with open('../News_Category_Dataset_sample.json') as file:
        news = file.readlines()  # Load JSON's lines into a list
        for i in range(0, number_of_items):
            try:
                while True:
                    item_id = bytes(json.loads(random.choice(news))['id'],
                                       encoding='utf-8')  # Select a random item from the list
                    # Check if the new random item is not already in the topic
                    if item_id not in existing_items:
                        break
                producer.send(topic_name, key=key, value=item_id)  # Publish item
                producer.flush()
                print(f'Publishing item: {item_id}\n')
            except Exception as ex:
                print('Exception in publishing item')
                print(str(ex))

    if producer is not None:
        producer.close()


if __name__ == '__main__':
    items_topic = 'items'
    key = bytes('article', encoding='utf-8')
    print(f"Item Producer\nItems' topic: {items_topic}")

    # Menu
    while True:
        selection = input("\n\n1. Manual\n2. Auto\n\nQuit: 'q'\n> ")

        # Exit the loop - Terminate the application
        if selection == 'q':
            break

        # Manual mode
        if selection == '1':
            items_num = input('Number of items: ')
            publish_item(items_topic, key, int(items_num))

        # Auto mode
        if selection == '2':
            while True:
                publish_item(items_topic, key, 1)
                time.sleep(round(random.uniform(5,10),1))
