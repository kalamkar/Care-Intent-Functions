import base64
import config
import cipher
import datetime
import flask
import hashlib
import json
import requests
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7

ALLOW_HEADERS = ['Accept', 'Authorization', 'Cache-Control', 'Content-Type', 'Cookie', 'Expires', 'Origin', 'Pragma',
                 'Access-Control-Allow-Headers', 'Access-Control-Request-Method', 'Access-Control-Request-Headers',
                 'Access-Control-Allow-Credentials', 'X-Requested-With']

DATA_PROVIDER_ACTION = {
    'type': 'DataProvider',
    'priority': 10,
    'condition': '{{schedule}}',
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


def handle_auth(request):
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


def create_polling(payload):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path('careintent', 'us-central1', payload['provider'])

    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': 'https://us-central1-careintent.cloudfunctions.net/process-task',
            'oidc_token': {'service_account_email': 'careintent@appspot.gserviceaccount.com'},
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        }
    }
    response = client.create_task(request={'parent': queue, 'task': task})
    print("Created task {}".format(response.name))
    return response.name


def oauth(request, _):
    state = cipher.parse_auth_token(request.args.get('state'))
    provider = state['provider']
    data = {'client_id': config.PROVIDERS[provider]['client_id'],
            'client_secret': config.PROVIDERS[provider]['client_secret'],
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    auth_response = requests.post(config.PROVIDERS[provider]['url'], data=data)
    print(auth_response.content)

    db = firestore.Client()
    action_doc = db.collection('persons').document(state['person_id'])\
        .collection('actions').document(state['provider']).get()
    if action_doc.exists and 'task_id' in action_doc.to_dict():
        tasks_v2.CloudTasksClient().delete_task(name=action_doc.get('task_id'))

    action = DATA_PROVIDER_ACTION | auth_response.json()
    action['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=action['expires_in'])
    action['task_id'] = create_polling(state)
    action_doc.reference.set(action)

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
    topic_path = publisher.topic_path(PROJECT_ID, 'message')
    url = 'https://us-central1-%s.cloudfunctions.net/auth/verify/%s' % (PROJECT_ID, verify_token)
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'receiver': contact,
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
