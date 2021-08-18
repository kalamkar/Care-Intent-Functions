import common
import config
import croniter
import datetime
import json
import pytz

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2


def handle_task(request):
    body = request.get_json()
    print(body)

    db = firestore.Client()
    action_doc, group_id, person_id = None, None, None
    action_id = body['action_id']
    if 'group_id' in body:
        group_id = body['group_id']
        action_doc = db.collection('groups').document(group_id).collection('actions').document(action_id).get()
    elif 'person_id' in body:
        person_id = body['person_id']
        action_doc = db.collection('persons').document(person_id).collection('actions').document(action_id).get()

    if not action_doc:
        print('Missing action for %s' % json.dumps(body))
        return

    action = action_doc.to_dict()
    if 'schedule' in action:
        now = datetime.datetime.utcnow()
        now = now.astimezone(pytz.timezone(action['timezone'])) if 'timezone' in action else now
        cron = croniter.croniter(action['schedule'], now)
        task_id = schedule_task(body, cron.get_next(datetime.datetime), 'actions')
        action_doc.reference.update({'task_id': task_id})

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    if group_id:
        for member in common.get_children_ids({'type': 'group', 'value': group_id}, 'member', db):
            if not member or member['type'] != 'person':
                continue
            data = {
                'time': datetime.datetime.utcnow().isoformat(),
                'sender': member,
                'tags': ['source:schedule'],
                'content_type': 'application/json',
                'content': {'group_id': group_id, 'action_id': action_id}
            }
            publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    elif person_id:
        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': {'type': 'person', 'value': person_id},
            'tags': ['source:schedule'],
            'content_type': 'application/json',
            'content': {'person_id': person_id, 'action_id': action_id}
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    return 'OK'


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
