import flask
import json
import uuid

from google.cloud import bigquery
from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7

ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']

RESOURCES = ['persons', 'relations', 'orgs', 'actions', 'content']


def api(request):
    response = flask.make_response()
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'

    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'GET,POST,PATCH,DELETE'
        response.headers['Access-Control-Allow-Headers'] = ', '.join(ALLOW_HEADERS)
        response.status_code = 204
        return response

    tokens = request.path.split('/')
    if len(tokens) < 3:
        response.status_code = 404
        return response

    # TODO: Check authentication and authorization

    if tokens[2] in RESOURCES:
        db = firestore.Client()
        collection = db.collection(tokens[2])
        if request.method == 'GET':
            response.content = flask.jsonify(collection.document(tokens[3]).get().to_dict())
        elif request.method == 'POST':
            doc_ref = collection.document(str(uuid.uuid4()))
            doc_ref.set(request.json)
        elif request.method == 'PATCH':
            doc_ref = collection.document(tokens[3])
            doc_ref.update(request.json)
    if tokens[2] == 'data' and len(tokens) >= 5:
        bq = bigquery.Client()
        query = 'SELECT time, number, value FROM careintent.live.tsdatav1, UNNEST(data) ' \
                'WHERE source.id = "{source}" AND name = "{name}" ' \
                'AND time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {seconds} second) ' \
                'ORDER BY time'. \
            format(source=tokens[3], name=tokens[4], seconds=request.args.get('seconds', '86400'))
        data = []
        for row in bq.query(query):
            data.append((row['time'], row['number'] or row['value']))

        response.content = flask.jsonify({'data': data})

    return response
