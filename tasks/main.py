import config
import datetime
import dateutil.parser
import providers
import json
import pytz
import requests

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from urllib.parse import urlencode


PROVIDERS = {'dexcom': providers.get_dexcom_data,
             'google': providers.get_google_data}


def handle_task(request):
    print(request.json)

    db = firestore.Client()
    person_ref = db.collection('persons').document(request.json['person-id'])
    provider_ref = person_ref.collection('providers').document(request.json['provider'])
    provider = provider_ref.get().to_dict()
    if 'expires' not in provider or \
            provider['expires'] < datetime.datetime.utcnow().astimezone(pytz.UTC):
        provider.update(get_access_token(provider['refresh_token'], request.json['provider']))

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'data')
    last_sync = provider['last_sync'] if 'last_sync' in provider else None
    for row in PROVIDERS[request.json['provider']](provider['access_token'], last_sync, person_ref.id):
        row_time = dateutil.parser.parse(row['time']).astimezone(pytz.UTC)
        last_sync = max(row_time, last_sync) if last_sync else row_time
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))

    if 'last_sync' not in provider or not provider['last_sync'] or (last_sync and provider['last_sync'] < last_sync):
        provider['last_sync'] = last_sync

    provider_ref.update(provider)

    if 'repeat-secs' in request.json:
        create_polling(request.json)

    return 'OK'


def create_polling(payload):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path(config.PROJECT_ID, 'us-central1', payload['provider'])

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(datetime.datetime.utcnow() + datetime.timedelta(seconds=payload['repeat-secs']))

    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': 'https://us-central1-%s.cloudfunctions.net/process-task' % config.PROJECT_ID,
            'oidc_token': {'service_account_email': '%s@appspot.gserviceaccount.com' % config.PROJECT_ID},
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        },
        'schedule_time': timestamp
    }
    response = client.create_task(request={'parent': queue, 'task': task})
    print("Created task {}".format(response.name))


def get_access_token(refresh, provider_name):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = urlencode({
        'client_id': config.PROVIDERS[provider_name]['client_id'],
        'client_secret': config.PROVIDERS[provider_name]['client_secret'],
        'refresh_token': refresh,
        'grant_type': 'refresh_token',
        'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'
    })
    response = requests.post(config.PROVIDERS[provider_name]['url'], body, headers=headers)
    if response.status_code > 299:
        return {}
    provider = response.json()
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    return provider
