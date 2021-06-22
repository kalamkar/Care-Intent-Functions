import cipher
import datetime
import flask
import json
import requests

from google.cloud import firestore
from google.cloud import tasks_v2

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg',
                        'client_secret': 'NZ4sTh0n4X6AT0XE'},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}


def handle_auth(request):
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
