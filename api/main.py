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
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        origin = request.headers.get('origin')
        response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET,POST,PATCH,DELETE'
        response.headers['Access-Control-Allow-Headers'] = ', '.join(ALLOW_HEADERS)
        response.status_code = 204
        return response

    tokens = request.path.split('/')
    print(tokens)

    # TODO: Check authentication and authorization

    if len(tokens) >= 2 and tokens[1] in RESOURCES:
        db = firestore.Client()
        collection = db.collection(tokens[1])
        if request.method == 'GET' and len(tokens) >= 3:
            response = flask.jsonify(collection.document(tokens[2]).get().to_dict())
        elif request.method == 'POST':
            doc_ref = collection.document(str(uuid.uuid4()))
            doc_ref.set(request.json)
        elif request.method == 'PATCH' and len(tokens) >= 3:
            doc_ref = collection.document(tokens[2])
            doc_ref.update(request.json)
    elif len(tokens) >= 3 and tokens[1] == 'data':
        bq = bigquery.Client()
        names = request.args.getlist('name')
        seconds = request.args.get('seconds', '86400')
        query = 'SELECT time, duration, name, number, value ' \
                'FROM {project}.live.tsdatav1, UNNEST(data) WHERE source.id = "{source}" AND name IN ({names}) ' \
                'AND time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {seconds} second) ' \
                'ORDER BY time'. \
            format(project=PROJECT_ID, source=tokens[2], names=str(names)[1:-1], seconds=seconds)
        print(query)
        rows = []
        for row in bq.query(query):
            rows.append({'time': row['time'].isoformat(),
                         'duration': row['duration'],
                         'name': row['name'],
                         'number': row['number'],
                         'value': row['value']})
        response = flask.jsonify({'rows': rows})
    elif len(tokens) >= 3 and tokens[1] == 'messages':
        bq = bigquery.Client()
        db = firestore.Client()
        person_ref = db.collection('persons').document(tokens[2]).get()
        values = [i['value'] for i in filter(lambda i: i['active'], person_ref.get('identifiers'))]
        seconds = request.args.get('seconds', '86400')
        query = 'SELECT time, status, tags, content, content_type ' \
                'FROM {project}.live.messages WHERE sender.value IN ({values}) ' \
                'AND time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {seconds} second) ' \
                'ORDER BY time'. \
            format(project=PROJECT_ID, values=str(values)[1:-1], seconds=seconds)
        print(query)
        rows = []
        for row in bq.query(query):
            rows.append({'time': row['time'].isoformat(),
                         'status': row['status'],
                         'tags': row['tags'],
                         'content': row['content'],
                         'content_type': row['content_type']})
        response = flask.jsonify({'rows': rows})
    else:
        response.status_code = 404

    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'

    return response
