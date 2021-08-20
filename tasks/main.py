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
        next_run_time = timestamp_pb2.Timestamp()
        next_run_time.FromDatetime(cron.get_next(datetime.datetime))
        task_id = common.schedule_task(body, tasks_v2.CloudTasksClient(), timestamp=next_run_time)
        action_doc.reference.update({'task_id': task_id})

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    member_ids = []
    if group_id:
        member_ids = common.get_children_ids({'type': 'group', 'value': group_id}, 'member', db)
    elif person_id:
        member_ids = [{'type': 'person', 'value': person_id}]

    for member in member_ids:
        if not member or member['type'] != 'person':
            continue
        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': member,
            'status': 'internal',
            'tags': ['source:schedule'],
            'content_type': 'application/json',
            'content': {'group_id' if group_id else 'person_id': group_id if group_id else person_id,
                        'action_id': action_id}
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    return 'OK'
