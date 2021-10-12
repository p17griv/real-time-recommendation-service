import random
import time
from kafka_connect import connect_kafka_producer, connect_kafka_consumer


class User:
    # Constructor
    def __init__(self, previous_users):
        while True:
            user_id = random.randint(0, 1000)
            # Check if user already exists
            if user_id not in previous_users:
                self.user_id = user_id
                break

    # Create and publish interactions for the user
    def create_interaction(self, items, key, interactions_topic):
        producer = connect_kafka_producer()

        item = random.choice(items)  # Get a random item
        item = str(item)[2:].replace("'", "")
        interaction = bytes(f'{self.user_id}:{item}', encoding='utf-8')

        try:
            # Publish interaction
            print(interaction)
            producer.send(interactions_topic, interaction, key)
            producer.flush()
            print(f'Publishing interaction: {self.user_id} -> {item}\n')
        except Exception as ex:
            print('Exception in publishing interaction')
            print(str(ex))


if __name__ == '__main__':
    items_topic = 'items'
    interactions_topic = 'interactions'
    key = bytes('interaction', encoding='utf-8')
    print(f"User Interactions Producer\nItems' topic: {items_topic}\nInteractions' topic: {interactions_topic}")
    users = []

    consumer = connect_kafka_consumer(items_topic, 'earliest')
    # Get all items in the topic
    items = []

    for msg in consumer:
        print(msg)
        items.append(msg.value)
    consumer.close()

    # Menu
    while True:
        selection = input("\n\n1. Create users\n2. Create interactions\n\nQuit: 'q'\n> ")

        # Exit the loop - Terminate the application
        if selection == 'q':
            break

        if selection == '1':
            users_number = input('Number of users: ')
            for user in range(int(users_number)):
                users.append(User(users))
                print(f'\rCreating users {user + 1}/{users_number}', end='', flush=True)

        if selection == '2':
            if not users:
                print('Create some users first\n')
            else:
                while True:
                    random.choice(users).create_interaction(items, key, interactions_topic)
                    time.sleep(round(random.uniform(0,3),1))
