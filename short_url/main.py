import flask
from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def main(request):
    db = firestore.Client()
    url = db.collection('urls').document(request.path[1:]).get()
    response = flask.make_response()
    if not url or not url.get('redirect'):
        response.status_code = 404
        return response

    return flask.redirect(url.get('redirect'), 302)

