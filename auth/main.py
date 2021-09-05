import base64
import config
import common
import cipher
import datetime
import flask
import hashlib
import json
import logging
import requests
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']

DATA_PROVIDER_ACTION = {
    'type': 'DataProvider',
    'priority': 10,
    'condition': '{{scheduled}}',
    'access_token': None,
    'refresh_token': None,
    'expires': datetime.datetime.utcnow(),
    'last_sync': None,
    'params': {
        'name': '$action.id',
        'access_token': '$action.access_token',
        'refresh_token': '$action.refresh_token',
        'expires': '$action.expires',
        'last_sync': '$action.last_sync',
        'source_id': '$sender.id'
    }
}


def main(request):
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
    if not tokens:
        response.status_code = 404
    elif tokens[-1] == '':
        response = oauth(request, response)
    elif tokens[-1] == 'signup':
        response = signup(request, response)
    elif tokens[-1] == 'login':
        response = login(request, response)
    elif tokens[-1] == 'verify':
        response = verify(request, response)

    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'
    return response


def oauth(request, _):
    state = cipher.parse_auth_token(request.args.get('state'))
    data = {'client_id': config.PROVIDERS[state['action_id']]['client_id'],
            'client_secret': config.PROVIDERS[state['action_id']]['client_secret'],
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    auth_response = requests.post(config.PROVIDERS[state['action_id']]['url'], data=data)
    logging.info(auth_response.content)

    action = DATA_PROVIDER_ACTION | auth_response.json() | state
    action['expires'] = datetime.datetime.utcnow() \
                        + datetime.timedelta(seconds=action['expires_in'] if 'expires_in' in action else 0)
    create_action(action, state['action_id'], firestore.Client(), tasks_v2.CloudTasksClient(),
                  {'type': 'person', 'value': state['person_id']})

    return flask.redirect('https://www.careintent.com', 302)


def signup(request, _):
    db = firestore.Client()
    identifier = request.json['identifier']
    hashpass = base64.b64encode(hashlib.sha256(request.json['password'].encode('utf-8')).digest()).decode('utf-8')
    id_type = 'email' if '@' in identifier else 'phone'
    contact = {'type': id_type, 'value': identifier, 'active': True}
    person_ref = db.collection('persons').where('identifiers', 'array_contains', contact)
    persons = list(person_ref.get())
    if len(persons) > 0:
        response = flask.jsonify({'status': 'error', 'message': 'Identifier already exists'})
        response.status_code = 409
        return response

    verify_token = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
    del contact['active']
    person = {'identifiers': [contact], 'name': request.json['name'],
              'login': {'verify': verify_token, 'id': identifier, 'hashpass': hashpass,
                        'signup_time': datetime.datetime.utcnow()}}
    person_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
    db.collection('persons').document(person_id).set(person)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')
    url = 'https://us-central1-%s.cloudfunctions.net/auth/verify/%s' % (config.PROJECT_ID, verify_token)
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'receiver': contact,
        'status': 'sent',
        'tags': ['source:auth'],
        'content_type': 'text/plain',
        'content': 'Verify signup %s' % url
    }
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')

    return flask.jsonify({'status': 'ok', 'message': 'Success'})


def verify(request, response):
    tokens = request.path.split('/')
    db = firestore.Client()
    person_ref = db.collection('persons').where('login.verify', '==', tokens[2])
    persons = list(person_ref.get())
    if len(persons) == 0:
        response.status_code = 403
        return response
    person = persons[0].to_dict()
    for identifier in person['identifiers']:
        if identifier['value'] == person['login']['id']:
            identifier['active'] = True
    del person['login']['verify']
    db.collection('persons').document(persons[0].id).update(person)
    return flask.redirect('https://app.careintent.com', 302)


def login(request, _):
    db = firestore.Client()
    identifier = request.json['identifier']
    hashpass = base64.b64encode(hashlib.sha256(request.json['password'].encode('utf-8')).digest()).decode('utf-8')
    id_type = 'email' if '@' in identifier else 'phone'
    contact = {'type': id_type, 'value': identifier, 'active': True}
    person_ref = db.collection('persons').where('identifiers', 'array_contains', contact)
    persons = list(person_ref.get())
    if len(persons) == 0:
        response = flask.jsonify({'status': 'error', 'message': 'Not found'})
        response.status_code = 404
        return response
    person = persons[0].to_dict()
    if person['login']['hashpass'] != hashpass:
        response = flask.jsonify({'status': 'error', 'message': 'Forbidden'})
        response.status_code = 403
        return response
    person['login']['id'] = identifier
    person['login']['time'] = datetime.datetime.utcnow()
    person['login']['token'] = str(uuid.uuid4())
    db.collection('persons').document(persons[0].id).update(person)
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=14)
    return flask.jsonify({'status': 'ok', 'token': person['login']['token'], 'expiry': expiry.isoformat()})


def create_action(action, action_id, db, tasks_client, parent_id):
    if not parent_id:
        logging.error('Missing group id and person id to create action')
        return
    action_doc = db.collection(common.COLLECTIONS[parent_id['type']]).document(parent_id['value']) \
        .collection('actions').document(action_id).get()
    if action_doc.exists and 'task_id' in action_doc.to_dict():
        try:
            tasks_client.delete_task(name=action_doc.get('task_id'))
        except:
            logging.warning('Could not delete task {}'.format(action_doc.get('task_id')))

    task = {'action_id': action_id, 'parent_id': parent_id}
    action['task_id'] = common.schedule_task(task, tasks_client)
    action_doc.reference.set(action)
