import base64
import datetime
import json
import flask
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
COLLECTIONS = {'person': 'persons'}
RELATION_TYPES = ['member_of', 'admin_of']


def api(request):
    response = flask.make_response()
    tokens = request.path.split('/')
    partner_id, resource_name, resource_id, sub_resource_name, sub_resource_id = None, None, None, None, None
    if len(tokens) == 3:
        _, partner_id, resource_name = tokens
    elif len(tokens) == 4:
        _, partner_id, resource_name, resource_id = tokens
    elif len(tokens) == 5:
        _, partner_id, resource_name, resource_id, sub_resource_name = tokens
    elif len(tokens) == 6:
        _, partner_id, resource_name, resource_id, sub_resource_name, sub_resource_id = tokens
    else:
        response.status_code = 400
        return response

    # TODO: Check authorization
    db = firestore.Client()
    try:
        auth_token = request.headers['Authorization'].split(' ')[1]
        group = db.collection('groups').document(partner_id).get()
        if auth_token not in [token['value'] for token in group.get('tokens')]:
            raise Exception
    except:
        response.status_code = 403
        return response

    if not resource_name:
        response.status_code = 400
        return response

    if resource_name not in COLLECTIONS:
        response.status_code = 404
        return response

    doc = None
    if request.method == 'GET' and resource_id and sub_resource_name and not sub_resource_id:
        doc = get_resources(resource_name, resource_id, sub_resource_name, db)
    elif request.method == 'GET' and resource_id:
        doc = get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db)
    elif request.method == 'PATCH' and resource_id:
        doc = update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, request.json, db)
    elif request.method == 'POST':
        if sub_resource_name == 'message' and resource_id:
            doc = send_message(resource_id, request.data.decode('utf-8'), group)
        elif request.json:
            relation = 'admin_of' if request.args.get('role') == 'admin' else 'member_of'
            doc = add_resource(resource_name, resource_id, sub_resource_name, request.json, relation, group.id, db)

    if not doc:
        response.status_code = 400
        return response

    response = flask.jsonify(doc)
    response.headers['Content-Type'] = 'application/json'
    return response


def get_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def get_resources(resource_name, resource_id, sub_resource_name, db):
    collection = db.collection(COLLECTIONS[resource_name]).document(resource_id)\
        .collection(COLLECTIONS[sub_resource_name])
    return {'results': [get_document_json(doc, sub_resource_name) for doc in collection.get()]}


def update_resource(resource_name, resource_id, sub_resource_name, sub_resource_id, resource, db):
    doc_ref = db.collection(COLLECTIONS[resource_name]).document(resource_id)
    if sub_resource_name and sub_resource_id:
        doc_ref = doc_ref.collection(COLLECTIONS[sub_resource_name]).document(sub_resource_id)
    doc_ref.update(resource)
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def add_resource(resource_name, resource_id, sub_resource_name, resource, relation, group_id, db):
    collection = db.collection(COLLECTIONS[resource_name])
    if resource_id and sub_resource_name:
        collection = collection.document(resource_id).collection(COLLECTIONS[sub_resource_name])

    if resource_name == 'person' and not sub_resource_name:
        person = get_person(resource['identifiers'], db)
        if person:
            return get_document_json(person, resource_name)
    doc_id = generate_id()
    doc_ref = collection.document(doc_id)
    doc_ref.set(resource)
    db.collection('relations').document(generate_id()).set({
        'source': {'type': resource_name, 'value': doc_id},
        'target': {'type': 'group', 'value': group_id},
        'type': relation
    })
    return get_document_json(doc_ref.get(), sub_resource_name or resource_name)


def send_message(person_id, content, group):
    db = firestore.Client()
    if ':' in person_id:
        id_type, id_value = person_id.split(':', 1)
        person_doc = get_person([{'type': id_type, 'value': id_value, 'active': True}], db)
        if not person_doc or id_type != 'phone':
            return None
        receiver = {'type': id_type, 'value': id_value}
    else:
        person_doc = db.collection('persons').document(person_id).get()
        receiver = list(filter(lambda i: i['type'] == 'phone', person_doc.get('identifiers')))[0]

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': group.get('identifiers')[0],
        'receiver': receiver,
        'tags': [],
        'content_type': 'text/plain',
        'content': content
    }
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')
    return {'message': 'ok'}


def get_document_json(doc, resource_type):
    doc_json = doc.to_dict()
    if resource_type == 'person':
        for name in doc_json.keys():
            if name not in ['identifiers', 'name']:
                del doc_json[name]
    if 'login' in doc_json:
        del doc_json['login']
    if 'tokens' in doc_json:
        del doc_json['tokens']
    doc_json['id'] = {'type': resource_type, 'value': doc.id}
    return doc_json


def get_person(identifiers, db):
    person_ref = db.collection('persons') \
        .where('identifiers', 'array_contains_any', identifiers)
    persons = list(person_ref.get())
    return persons[0] if len(persons) > 0 else None


def generate_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
