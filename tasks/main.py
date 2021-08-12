import config
import croniter
import datetime
import dateutil.parser
import providers
import json
import pytz
import query
import requests
import sys

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from urllib.parse import urlencode


PROVIDERS = {'dexcom': providers.get_dexcom_data,
             'google': providers.get_google_data}


def handle_task(request):
    body = request.get_json()
    print(body)
    if 'provider' in body:
        handle_provider(body)
    elif 'schedule' in body:
        handle_scheduled(body)
    return 'OK'


def handle_scheduled(body):
    group_id = body['group_id']
    action_id = body['action_id']

    cron = croniter.croniter(body['schedule'], datetime.datetime.utcnow())
    task_id = schedule_task(body, cron.get_next(datetime.datetime), 'actions')

    db = firestore.Client()
    action_ref = db.collection('groups').document(group_id).collection('actions').document(action_id)
    action_ref.update({'task_id': task_id})

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    members = query.get_relatives([], ['member_of'], {'type': 'group', 'value': group_id})
    for member in members:
        if member['type'] != 'person':
            continue
        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': member,
            'tags': [],
            'content_type': 'application/json',
            'content': {'group_id': group_id, 'action_id': action_id}
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))


def handle_provider(body):
    db = firestore.Client()
    person_ref = db.collection('persons').document(body['person-id'])
    provider_ref = person_ref.collection('providers').document(body['provider'])
    provider = provider_ref.get().to_dict()
    if 'expires' not in provider or \
            provider['expires'] < datetime.datetime.utcnow().astimezone(pytz.UTC):
        provider.update(get_access_token(provider['refresh_token'], body['provider']))

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'data')
    last_sync = provider['last_sync'] if 'last_sync' in provider else None
    for row in PROVIDERS[body['provider']](provider['access_token'], last_sync, person_ref.id):
        row_time = dateutil.parser.parse(row['time']).astimezone(pytz.UTC)
        last_sync = max(row_time, last_sync) if last_sync else row_time
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))

    if 'last_sync' not in provider or not provider['last_sync'] or (last_sync and provider['last_sync'] < last_sync):
        provider['last_sync'] = last_sync

    if 'repeat-secs' in body:
        next_run_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=body['repeat-secs'])
        provider['task_id'] = schedule_task(body, next_run_time, body['provider'])

    provider_ref.update(provider)


def schedule_task(payload, next_run_time, queue_name):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path(config.PROJECT_ID, 'us-central1', queue_name)

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(next_run_time)

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
    return response.name


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
        print(response.content)
        return response.json()
    provider = response.json()
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    return provider
