import base64
import datetime
import dateutil.parser
import json
import flask
import uuid

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
JSON_CACHE_SECONDS = 600
ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']
COLLECTIONS = {'person': 'persons', 'group': 'groups', 'message': 'messages', 'schedule': 'schedules'}
RELATION_TYPES = ['member_of', 'admin_of']


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
    resource_name, resource_id, sub_resource_name, sub_resource_id = None, None, None, None
    if len(tokens) == 2:
        _, resource_name = tokens
    elif len(tokens) == 3:
        _, resource_name, resource_id = tokens
    elif len(tokens) == 4:
        _, resource_name, resource_id, sub_resource_name = tokens
    elif len(tokens) == 5:
        _, resource_name, resource_id, sub_resource_name, sub_resource_id = tokens
    else:
        response.status_code = 400
        return response
    print(resource_name, resource_id, sub_resource_name, sub_resource_id)

    # TODO: Check authorization
    db = firestore.Client()
    try:
        auth_token = request.headers['Authorization'].split(' ')[1]
        user = list(db.collection('persons').where('login.token', '==', auth_token).get())[0]
        if not user:
            raise Exception
    except:
        response.status_code = 401
        return response

    if not resource_name:
        response.status_code = 400
        return response

    if resource_name not in COLLECTIONS or \
            (sub_resource_name and sub_resource_name not in (list(COLLECTIONS.keys()) + ['data', 'relation'])):
        response.status_code = 404
        return response

    doc = None
    if request.method == 'GET' and sub_resource_name == 'relation' and resource_id:
        resource = {'type': resource_name, 'value': resource_id}
        relation_type = request.args.get('relation_type')
        resource_type = request.args.get('resource_type')
        doc = query(resource, relation_type, resource_type)
    elif request.method == 'GET' and resource_name == 'person' and resource_id and sub_resource_name == 'data':
        start_time, end_time = get_start_end_times(request)
        doc = {'results': get_rows(start_time, end_time, resource_id, request.args.getlist('name'))}
    elif request.method == 'GET' and resource_id and sub_resource_name and not sub_resource_id:
        doc = get_resources(resource_name, resource_id, sub_resource_name, db)
        if resource_name == 'person' and sub_resource_name == 'message':
            for qmessage in doc['results']:
                qmessage['status'] = 'queued'
            start_time, end_time = get_start_end_times(request)
            doc['results'].extend(get_messages(start_time, end_time, resource_id, request.args.get('both')))
    elif request.method == 'GET' and resource_id:
        resource_id = user.id if resource_id == 'me' else resource_id
        doc = get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db)
    elif request.method == 'PATCH' and resource_id:
        doc = update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, request.json, db)
    elif request.method == 'POST' and request.json:
        if resource_name == 'relation':
            doc = add_relation(request.json)
        elif sub_resource_name == 'message' and resource_id:
            doc = send_message(resource_id, request.json, user)
        else:
            doc = add_resource(resource_name, resource_id, sub_resource_name, request.json, user.id, db)

    if doc:
        response = flask.jsonify(doc)
    else:
        response.status_code = 404

    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'
    response.headers['Content-Type'] = 'application/json'
    response.headers['Cache-Control'] = 'max-age=%d' % JSON_CACHE_SECONDS
    return response


def query(resource, relation_type, resource_type):
    result_type = 'target' if resource_type == 'source' else 'source'
    results = []
    db = firestore.Client()
    for relation in db.collection('relations')\
            .where(resource_type, '==', resource).where('type', '==', relation_type).get():
        result_id = relation.get(result_type)
        doc = db.collection(result_id['type'] + 's').document(result_id['value']).get()
        results.append(get_document_json(doc, result_id['type']))

    return {'results': results}


def add_relation(relation):
    if 'source' not in relation or 'target' not in relation or 'type' not in relation\
            or relation['type'] not in RELATION_TYPES:
        return None

    db = firestore.Client()
    db.collection('relations').document(generate_id()).set(relation)
    return {'status': 'ok'}


def get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def get_resources(resource_name, resource_id, sub_resource_name, db):
    collection = db.collection(COLLECTIONS[resource_name]).document(resource_id) \
        .collection(COLLECTIONS[sub_resource_name])
    return {'results': [get_document_json(doc, sub_resource_name) for doc in collection.get()]}


def update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, resource, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    doc_ref.update(resource)
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def add_resource(resource_name, resource_id, sub_resource_name, resource, user_id, db):
    collection = db.collection(COLLECTIONS[resource_name])
    if resource_id and sub_resource_name:
        collection = collection.document(resource_id).collection(COLLECTIONS[sub_resource_name])

    if resource_name == 'person' and not sub_resource_name:
        person_ref = db.collection('persons') \
            .where('identifiers', 'array_contains_any', resource['identifiers'])
        persons = list(person_ref.get())
        if len(persons) > 0:
            return get_document_json(persons[0], resource_name)
    doc_id = generate_id()
    doc_ref = collection.document(doc_id)
    doc_ref.set(resource)
    db.collection('relations').document(generate_id()).set({
        'source': {'type': 'person', 'value': user_id},
        'target': {'type': resource_name, 'value': doc_id},
        'type': 'created'
    })
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def get_rows(start_time, end_time, source, names):
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
    return rows


def get_messages(start_time, end_time, person_id, both):
    bq = bigquery.Client()
    db = firestore.Client()
    person_doc = db.collection('persons').document(person_id).get()
    values = [i['value'] for i in filter(lambda i: i['active'], person_doc.get('identifiers'))]
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
    return rows


def send_message(person_id, message, user):
    db = firestore.Client()
    person_doc = db.collection('persons').document(person_id).get()
    if {**message['receiver'], 'active': True} not in person_doc.to_dict()['identifiers']:
        print('Invalid receiver {r} for person {pid}'.format(r=message['receiver'], pid=person_id))
        return None
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': 'person', 'value': user.id},
        'receiver': message['receiver'],
        'tags': message['tags'] if 'tags' in message else [],
        'content_type': 'text/plain',
        'content': message['content']
    }
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')
    return {'message': 'ok'}


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
