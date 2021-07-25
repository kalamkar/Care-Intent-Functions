import base64
import datetime
import dateutil.parser
import flask
import uuid

from google.cloud import bigquery
from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
JSON_CACHE_SECONDS = 600
ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']
RESOURCES = ['persons', 'groups', 'actions', 'contents']
RELATION_TYPES = ['member_of', 'admin_of']


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

    # TODO: Check authorization
    db = firestore.Client()
    try:
        auth_token = request.headers['Authorization'].split(' ')[1]
        user = list(db.collection('persons').where('login.token', '==', auth_token).get())[0]
    except:
        user = None

    if len(tokens) >= 2 and tokens[1] == 'query' and user:
        response = query(request, response, user)
    elif len(tokens) >= 2 and tokens[1] == 'relate' and user and request.json:
        response = add_relation(request, response, user)
    elif len(tokens) >= 2 and tokens[1] in RESOURCES and user:
        response = resources(request, response, user)
    elif len(tokens) >= 3 and tokens[1] == 'data':
        start_time, end_time = get_start_end_times(request)
        response = data(start_time, end_time, tokens[2], request.args.getlist('name'))
    elif len(tokens) >= 3 and tokens[1] == 'messages':
        start_time, end_time = get_start_end_times(request)
        response = messages(start_time, end_time, tokens[2], request.args.get('both'))
    else:
        response.status_code = 404

    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'
    response.headers['Content-Type'] = 'application/json'
    response.headers['Cache-Control'] = 'max-age=%d' % JSON_CACHE_SECONDS
    return response


def query(request, response, user):
    resource = request.args.get('resource').split(':', 1)
    resource = {'type': resource[0], 'value': resource[1]}
    relation_type = request.args.get('relation_type')
    resource_type = request.args.get('resource_type')
    result_type = 'target' if resource_type == 'source' else 'source'

    results = []
    db = firestore.Client()
    for relation in db.collection('relations')\
            .where(resource_type, '==', resource).where('type', '==', relation_type).get():
        result_id = relation.get(result_type)
        doc = db.collection(result_id['type'] + 's').document(result_id['value']).get()
        results.append(get_document_json(doc, result_id['type']))

    return flask.jsonify({'results': results})


def add_relation(request, response, user):
    relation = request.json
    if 'source' not in relation or 'target' not in relation or 'type' not in relation\
            or relation['type'] not in RELATION_TYPES:
        response.status_code = 400
        return response

    db = firestore.Client()
    db.collection('relations').document(generate_id()).set(relation)
    return flask.jsonify({'status': 'ok'})


def resources(request, response, user):
    tokens = request.path.split('/')
    db = firestore.Client()
    collection = db.collection(tokens[1])
    if request.method == 'GET' and len(tokens) >= 3:
        doc = user if tokens[2] == 'me' else collection.document(tokens[2]).get()
        response = flask.jsonify(get_document_json(doc, tokens[1][:-1]))
    elif request.method == 'POST':
        if tokens[1] == 'persons':
            person_ref = db.collection('persons')\
                .where('identifiers', 'array_contains_any', request.json['identifiers'])
            persons = list(person_ref.get())
            if len(persons) > 0:
                return flask.jsonify(get_document_json(persons[0], tokens[1][:-1]))
        doc_id = generate_id()
        doc_ref = collection.document(doc_id)
        doc_ref.set(request.json)
        db.collection('relations').document(generate_id()).set({
            'source': {'type': 'person', 'value': user.id},
            'target': {'type': tokens[1][:-1], 'value': doc_id},
            'type': 'admin_of' if tokens[1] == 'groups' else 'created'
        })
        response = flask.jsonify(get_document_json(doc_ref.get(), tokens[1][:-1]))
    elif request.method == 'PATCH' and len(tokens) >= 3:
        doc_ref = collection.document(tokens[2])
        doc_ref.update(request.json)
        response = flask.jsonify(get_document_json(doc_ref.get(), tokens[1][:-1]))
    return response


def data(start_time, end_time, source, names):
    bq = bigquery.Client()
    query = 'SELECT time, duration, name, number, value ' \
            'FROM {project}.live.tsdatav1, UNNEST(data) WHERE source.id = "{source}" AND name IN ({names}) ' \
            'AND TIMESTAMP("{start}") < time AND time < TIMESTAMP("{end}") ' \
            'ORDER BY time'. \
        format(project=PROJECT_ID, source=source, names=str(names)[1:-1], start=start_time, end=end_time)
    print(query)
    rows = []
    for row in bq.query(query):
        rows.append({'time': row['time'].isoformat(),
                     'duration': row['duration'],
                     'name': row['name'],
                     'number': row['number'],
                     'value': row['value']})
    return flask.jsonify({'rows': rows})


def messages(start_time, end_time, person_id, both):
    bq = bigquery.Client()
    db = firestore.Client()
    person_ref = db.collection('persons').document(person_id).get()
    values = [i['value'] for i in filter(lambda i: i['active'], person_ref.get('identifiers'))]
    if both:
        query = 'SELECT time, status, sender, receiver, tags, content, content_type ' \
                'FROM {project}.live.messages WHERE (sender.value IN ({values}) OR receiver.value IN ({values})) ' \
                'AND TIMESTAMP("{start}") < time AND time < TIMESTAMP("{end}") ' \
                'ORDER BY time'. \
            format(project=PROJECT_ID, values=str(values)[1:-1], start=start_time, end=end_time)
    else:
        query = 'SELECT time, status, sender, receiver, tags, content, content_type ' \
                'FROM {project}.live.messages WHERE sender.value IN ({values}) ' \
                'AND TIMESTAMP("{start}") < time AND time < TIMESTAMP("{end}") ' \
                'ORDER BY time'. \
            format(project=PROJECT_ID, values=str(values)[1:-1], start=start_time, end=end_time)
    print(query)
    rows = []
    for row in bq.query(query):
        rows.append({'time': row['time'].isoformat(),
                     'status': row['status'],
                     'sender': row['sender'],
                     'receiver': row['receiver'],
                     'tags': row['tags'],
                     'content': row['content'],
                     'content_type': row['content_type']})
    return flask.jsonify({'rows': rows})


def get_start_end_times(request):
    start_time = request.args.get('start')
    start_time = dateutil.parser.parse(start_time) \
        if start_time else datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)
    end_time = request.args.get('end')
    end_time = dateutil.parser.parse(end_time) if end_time else datetime.datetime.utcnow()
    return start_time.isoformat(), end_time.isoformat()


def get_document_json(doc, id_type):
    doc_json = doc.to_dict()
    if 'login' in doc_json:
        del doc_json['login']
    doc_json['id'] = {'type': id_type, 'value': doc.id}
    return doc_json


def generate_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
