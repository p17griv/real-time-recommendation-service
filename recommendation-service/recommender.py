from scipy.sparse import load_npz
from flask import Flask, request, abort
import pickle

app = Flask(__name__)


@app.route('/', methods=['GET'])
def send_recommendations():
    try:
        if request.args.get('userId') is not None and request.args.get('recommendationsNumber') is not None:
            result = make_recommendations(int(request.args.get('userId')), int(request.args.get('recommendationsNumber')))
            if result == -1:
                raise ValueError
            else:
                return result
        else:
            raise ValueError
    except ValueError:
        abort(400, 'Bad Request\nCheck Parameters')


# Recommend N items for the target user
def make_recommendations(target_user_id, n):
    # Load the stored user-item matrix
    sparse_user_item = load_npz("sparse_user_item.npz")

    # Load the stored recommendations model
    with open('model.sav', 'rb') as pickle_in:
        model = pickle.load(pickle_in)

    try:
        # Make recommendations using the model
        recommended = model.recommend(target_user_id, sparse_user_item, n, filter_already_liked_items=True)
    except IndexError:
        return -1

    recommended_items = []
    for item in recommended:
        recommended_items.append(int(item[0]))
    return {'user_id': target_user_id,
            'recommended_items': recommended_items}


if __name__ == '__main__':
    app.run(debug=True)
