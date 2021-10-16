import flask
from google.cloud import firestore

app = flask.Flask(__name__)


@app.route('/<path>')
def main(path):
    db = firestore.Client()
    url = db.collection('urls').document(path).get()
    response = flask.make_response()
    if not url or not url.get('redirect'):
        response.status_code = 404
        return response

    return flask.redirect(url.get('redirect'), 302)
