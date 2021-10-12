from scipy.sparse import save_npz
from kafka_connect import connect_kafka_consumer
import threading
import scipy.sparse as sparse
import pandas as pd
import implicit
import pickle
import queue
import argparse


# Create and store an ALS recommendations model
def create_recommendations_model(users_column, items_column, interactions_column):
    # The implicit library expects data as a item-user matrix
    # Create one matrix for fitting the model (item-user)
    sparse_item_user = sparse.csr_matrix((interactions_column.astype(float), (items_column, users_column)))

    # Transposing the item-user matrix to create a matrix for recommendations (user-item)
    sparse_user_item = sparse_item_user.T.tocsr()

    # Save the user-item matrix for making recommendations asynchronously
    save_npz("sparse_user_item.npz", sparse_user_item)

    # Initialize the als model
    model = implicit.als.AlternatingLeastSquares(factors=20, regularization=0.1, iterations=20)

    # Calculate the confidence by multiplying it by our alpha value.
    alpha_val = 15
    data_conf = (sparse_item_user * alpha_val).astype('double')

    # Fit the model
    model.fit(data_conf)

    # Serialize and save the model
    with open('model.sav', 'wb') as pickle_out:
        pickle.dump(model, pickle_out)


if __name__ == '__main__':
    # Defining program arguments
    arg_parser = argparse.ArgumentParser(description='Continuously builds an ALS model for generating recommendations, '
                                                     'using a data stream of user-item interactions from a Kafka '
                                                     'topic')
    arg_parser.add_argument('interactions_topic', type=str, help='the kafka topic with user-item interactions')
    arg_parser.add_argument('retrain_frequency', type=int, help='the number of the received messages after '
                                                                'which the model will be re-trained')
    arg_parser.add_argument('window_length', type=int, help='the number of the latest received messages that '
                                                            'will be used to train the model')
    arg_parser.add_argument('-b', '--from-beginning', action='store_true', help='start with getting all messages '
                                                                                'stored in the topic from the beginning')
    # Parsing the given arguments
    args = arg_parser.parse_args()
    interactions_topic = args.interactions_topic
    freq = args.retrain_frequency
    window_length = args.window_length
    if args.from_beginning:
        consumer = connect_kafka_consumer(interactions_topic, 'earliest')
    else:
        consumer = connect_kafka_consumer(interactions_topic, 'latest')

    msg_count = 1
    init = True
    q = queue.Queue(maxsize=window_length)  # A FIFO queue

    while True:
        # Keep getting new interactions from kafka topic from the beginning
        for msg in consumer:
            # Replace oldest event with the latest if 'window_length' limit has been reached
            if q.full():
                q.get()
                q.put(msg)
            else:
                q.put(msg)
            # Retrain model every 'freq' events with the last 'window_length' events
            if msg_count == freq:
                counts = dict()
                users = []
                items = []
                interactions = []
                # Get the last 'window_length' events
                for lmsg in q.queue:
                    lmsg = str(lmsg.value)[2:].replace("'", "")

                    # Count the occurrences of each interaction
                    if lmsg in counts:
                        counts[lmsg] += 1
                    else:
                        counts[lmsg] = 1

                    users.append(int(lmsg.split(':')[0]))
                    items.append(int(lmsg.split(':')[1]))
                    interactions.append(counts[lmsg])
                # Data to pandas dataframe
                df = pd.DataFrame(list(zip(users, items, interactions)), columns=['User', 'Item', 'Interaction'])

                # and if there is no active thread for training
                if init or not t1.is_alive():
                    # Sparse matrix columns
                    users_col = df['User']
                    items_col = df['Item']
                    interactions_col = df['Interaction']

                    # Retrain the model on a different thread
                    t1 = threading.Thread(target=create_recommendations_model,
                                          args=(users_col, items_col, interactions_col))
                    t1.start()
                    init = False
                msg_count = 0

            msg_count += 1

    consumer.close()
