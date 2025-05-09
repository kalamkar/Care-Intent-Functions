import base64
import common
import config
import datetime
import dateutil.parser
import json
import flask
import logging
import uuid

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1

from common import COLLECTIONS

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

JSON_CACHE_SECONDS = 600
ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']


def main(request):
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
    logging.info('{} /{}/{}/{}/{}'.format(request.method, resource_name, resource_id,
                                          sub_resource_name, sub_resource_id))
    log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'api.' + request.method.lower(),
           'resources': [{'type': resource_name, 'value': resource_id}]}
    if sub_resource_id:
        log['resources'].append({'type': sub_resource_name, 'value': sub_resource_id if sub_resource_id else ''})
    errors = bigquery.Client().insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
    if errors:
        logging.warning(errors)

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
    if request.method == 'GET' and sub_resource_name in ['member', 'admin']:
        doc = list_resources(resource_name, resource_id, sub_resource_name, sub_resource_id)
    elif request.method == 'GET' and resource_name == 'person' and sub_resource_name == 'data' and resource_id:
        doc = {'results': []}
        if request.args.getlist('name'):
            start_time, end_time = get_start_end_times(request)
            doc['results'] = get_data_by_names(start_time, end_time, resource_id, request.args.getlist('name'))
        elif request.args.get('tag'):
            doc['results'] = get_data_by_tag(resource_id, request.args.get('tag'))
    elif request.method == 'GET' and resource_id and sub_resource_name and not sub_resource_id:
        doc = get_resources(resource_name, resource_id, sub_resource_name, db)
        if resource_name == 'person' and sub_resource_name == 'message':
            for qmessage in doc['results']:
                qmessage['status'] = 'queued'
            start_time, end_time = get_start_end_times(request)
            doc['results'].extend(get_messages(start_time, end_time, resource_id, request.args.get('both'),
                                               request.args.get('tag')))
    elif request.method == 'GET' and resource_id:
        resource_id = user.id if resource_id == 'me' else resource_id
        doc = get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db)
    elif request.method == 'PATCH' and resource_id:
        doc = update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, request.json, db)
    elif request.method == 'DELETE' and sub_resource_id:
        doc = delete_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db)
    elif request.method == 'POST' and request.json:
        if sub_resource_name in ['member', 'admin'] and resource_id:
            doc = add_relation(resource_name, resource_id, sub_resource_name, request.json)
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


def list_resources(resource_name, resource_id, sub_resource_name, sub_resource_id):
    results = []
    db = firestore.Client()
    if sub_resource_id and (not resource_id or resource_id in ['any', 'all']):
        # Get all the parents of the sub_resource_name:sub_resource_id
        relation_query = db.collection_group(COLLECTIONS[sub_resource_name]).where('id.value', '==', sub_resource_id)
        for relative in relation_query.stream():
            parent = relative.reference.parent.parent.get()
            parent_type = parent.reference.path.split('/')[0][:-1]
            if parent_type != resource_name:
                continue
            results.append(get_document_json(parent))
    elif resource_id and (not sub_resource_id or sub_resource_id in ['any', 'all']):
        # Get all the children
        relation_query = db.collection(COLLECTIONS[resource_name]).document(resource_id)\
            .collection(COLLECTIONS[sub_resource_name])
        for doc in relation_query.stream():
            relative = db.collection(COLLECTIONS[doc.get('id.type')]).document(doc.get('id.value')).get()
            results.append(get_document_json(doc) | get_document_json(relative))
    return {'results': results}


def add_relation(resource_name, resource_id, sub_resource_name, identifier):
    if 'type' not in identifier or 'value' not in identifier:
        return None
    db = firestore.Client()
    data = common.add_child(identifier, {'type': resource_name, 'value': resource_id}, sub_resource_name, db)
    return {'status': 'ok'} if data else None


def get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    return get_document_json(doc_ref.get())


def get_resources(resource_name, resource_id, sub_resource_name, db):
    collection = db.collection(COLLECTIONS[resource_name]).document(resource_id) \
        .collection(COLLECTIONS[sub_resource_name])
    return {'results': [get_document_json(doc) for doc in collection.get()]}


def update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, resource, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    elif resource_name == 'person' and 'identifiers' in resource:
        # Check if the new identifiers exist for someone else
        for person in db.collection('persons')\
                .where('identifiers', 'array_contains_any', resource['identifiers']).stream():
            if person.reference.path != doc_ref.path:
                return None
    doc_ref.update(resource)
    return get_document_json(doc_ref.get())


def delete_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db):
    db.collection(COLLECTIONS[resource_name]).document(resource_id)\
        .collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id).delete()
    return {'status': 'ok'}


def add_resource(resource_name, resource_id, sub_resource_name, resource, user_id, db):
    collection = db.collection(COLLECTIONS[resource_name])
    if resource_id and sub_resource_name:
        collection = collection.document(resource_id).collection(COLLECTIONS[sub_resource_name])

    if resource_name == 'person' and not sub_resource_name:
        person_ref = db.collection('persons') \
            .where('identifiers', 'array_contains_any', resource['identifiers'])
        persons = list(person_ref.get())
        if len(persons) > 0:
            return get_document_json(persons[0])
    doc_id = generate_id()
    doc_ref = collection.document(doc_id)
    doc_ref.set(resource)
    if resource_name == 'group' and not resource_id:
        db.collection(COLLECTIONS[resource_name]).document(doc_id).collection('admins')\
            .document('person:' + user_id).set({'id': {'type': 'person', 'value': user_id}})
    return get_document_json(doc_ref.get())


def get_data_by_names(start_time, end_time, source, names):
    bq = bigquery.Client()
    query = 'SELECT time, duration, name, number, value ' \
            'FROM {project}.live.tsdata, UNNEST(data) WHERE source.value = "{source}" AND name IN ({names}) ' \
            'AND TIMESTAMP("{start}") < time AND time < TIMESTAMP("{end}") ' \
            'ORDER BY time'. \
        format(project=config.PROJECT_ID, source=source, names=str(names)[1:-1], start=start_time, end=end_time)
    logging.info(query)
    rows = []
    for row in bq.query(query):
        rows.append({'time': row['time'].isoformat(),
                     'duration': row['duration'],
                     'name': row['name'],
                     'number': row['number'],
                     'value': row['value']})
    return rows


def get_data_by_tag(source, tag):
    bq = bigquery.Client()
    query = '''SELECT time, data
               FROM {project}.live.tsdata WHERE source.value = "{source}" AND "{tag}" IN UNNEST(tags)
               ORDER BY time'''.format(project=config.PROJECT_ID, source=source, tag=tag)
    logging.info(query)
    rows = []
    for row in bq.query(query):
        rows.append({'time': row['time'].isoformat(), 'data': row['data']})
    return rows


def get_messages(start_time, end_time, person_id, both, tag):
    bq = bigquery.Client()
    db = firestore.Client()
    person_doc = db.collection('persons').document(person_id).get()
    values = [i['value'] for i in person_doc.get('identifiers')]
    values.append(person_doc.id)
    query = 'SELECT time, status, sender, receiver, tags, content, content_type '\
            + 'FROM {project}.live.messages WHERE '\
            + ('(sender.value IN ({values}) OR receiver.value IN ({values})) '
               if both else 'sender.value IN ({values}) ')\
            + 'AND TIMESTAMP("{start}") < time AND time < TIMESTAMP("{end}") '\
            + 'AND "source:schedule" NOT IN UNNEST(tags) '\
            + ('AND "{tag}" IN UNNEST(tags) ' if tag else '')\
            + 'ORDER BY time'
    query = query.format(project=config.PROJECT_ID, values=str(values)[1:-1], start=start_time, end=end_time, tag=tag)
    logging.info(query)
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
    if 'receiver' in message and message['receiver'] not in person_doc.to_dict()['identifiers'] \
            and message['receiver'] != {'type': 'person', 'value': person_id}:
        logging.error('Invalid receiver {r} for person {pid}'.format(r=message['receiver'], pid=person_id))
        return None
    receiver = message['receiver'] if 'receiver' in message else common.filter_identifier(person_doc, 'phone')
    if not receiver:
        logging.warning('Missing receiver')
        return {'message': 'Missing receiver'}
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': 'person', 'value': user.id},
        'receiver': receiver,
        'status': 'sent' if 'status' not in message else message['status'],
        'tags': message['tags'] if 'tags' in message else ['source:api'],
        'content_type': 'text/plain' if 'content_type' not in message else message['content_type'],
        'content': message['content']
    }
    if data['status'] == 'sent':
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')
    else:
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return {'message': 'ok'}


def get_start_end_times(request):
    start_time = request.args.get('start')
    start_time = dateutil.parser.parse(start_time) \
        if start_time else datetime.datetime.utcnow() - datetime.timedelta(seconds=86400)
    end_time = request.args.get('end')
    end_time = dateutil.parser.parse(end_time) if end_time else datetime.datetime.utcnow()
    return start_time.isoformat(), end_time.isoformat()


def get_document_json(doc):
    doc_json = doc.to_dict()
    if not doc_json:
        return {}
    if 'login' in doc_json:
        del doc_json['login']
    doc_json['id'] = {'type': doc.reference.path.split('/')[0][:-1], 'value': doc.id}
    return doc_json


def generate_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
