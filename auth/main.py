import base64
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


PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg',
                        'client_secret': 'NZ4sTh0n4X6AT0XE'},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}


def handle_auth(request):
    tokens = request.path.split('/')
    if len(tokens) == 1:
        return oauth(request)
    elif len(tokens) >= 2 and tokens[1] == 'signup':
        return signup(request)
    elif len(tokens) >= 2 and tokens[1] == 'login':
        return login(request)
    elif len(tokens) >= 2 and tokens[1] == 'verify':
        return verify(request)
    return 404, 'Not Found'


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


def stop_polling(provider, task_id):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path('careintent', 'us-central1', provider)
    client.delete_task(name=queue + '/tasks/' + task_id)


def oauth(request):
    state = cipher.parse_auth_token(request.args.get('state'))
    provider = state['provider']
    data = {'client_id': PROVIDERS[provider]['client_id'],
            'client_secret': PROVIDERS[provider]['client_secret'],
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    response = requests.post(PROVIDERS[provider]['url'], data=data)
    print(response.content)

    db = firestore.Client()
    person_ref = db.collection('persons').document(state['person-id'])
    provider_ref = person_ref.collection('providers').document(state['provider'])
    provider = provider_ref.get()
    if provider and 'task_id' in provider:
        stop_polling(state['provider'], provider['task_id'])

    provider = response.json()
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    provider['task_id'] = create_polling(state)
    provider_ref.set(provider)

    return flask.redirect('https://www.careintent.com', 302)


def signup(request):
    db = firestore.Client()
    identifier = request.json['identifier']
    hashpass = base64.b64encode(hashlib.sha256(request.json['password'].encode('utf-8')).digest())
    id_type = 'email' if '@' in identifier else 'phone'
    contact = {'type': id_type, 'value': identifier, 'active': True}
    person_ref = db.collection('persons').where('identifiers', 'array_contains', contact)
    persons = list(person_ref.get())
    if len(persons) > 0:
        return flask.jsonify({'status': 'error', 'message': 'Identifier already exists'}), 409

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
        'content_type': 'text/plain',
        'content': 'Verify signup %s' % url
    }
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')

    return flask.jsonify({'status': 'ok', 'message': 'Success'}), 200


def verify(request):
    tokens = request.path.split('/')
    db = firestore.Client()
    person_ref = db.collection('persons').where('signup.token', '==', tokens[2])
    persons = list(person_ref.get())
    if len(persons) == 0:
        return 'Forbidden', 403
    person = persons[0].to_dict()
    for identifier in person['identifiers']:
        if identifier['value'] == person['signup']['id']:
            identifier['active'] = True
    del person['login']['verify']
    db.collection('persons').document(persons[0].id).update(person)
    return flask.redirect('https://app.careintent.com', 302)


def login(request):
    db = firestore.Client()
    identifier = request.json['identifier']
    hashpass = base64.b64encode(hashlib.sha256(request.json['password'].encode('utf-8')).digest())
    id_type = 'email' if '@' in identifier else 'phone'
    contact = {'type': id_type, 'value': identifier, 'active': True}
    person_ref = db.collection('persons').where('identifiers', 'array_contains', contact)
    persons = list(person_ref.get())
    if len(persons) == 0:
        return flask.jsonify({'status': 'error', 'message': 'Not found'}), 404
    person = persons[0].to_dict()
    if person['hashpass'] != hashpass:
        return flask.jsonify({'status': 'error', 'message': 'Forbidden'}), 403
    person['login']['id'] = identifier
    person['login']['time'] = datetime.datetime.utcnow()
    person['login']['token'] = str(uuid.uuid4())
    db.collection('persons').document(persons[0].id).update(person)
    expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=14)
    return flask.jsonify({'status': 'ok', 'token': person['login']['token'], 'expiry': expiry.isoformat()}), 200
