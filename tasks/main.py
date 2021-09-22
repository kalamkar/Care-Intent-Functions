import common
import config
import croniter
import datetime
import json
import logging
import pytz

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(request):
    body = request.get_json()
    logging.info(body)
    # expected {'action_id': action_id, 'policy': policy, 'target_id': target_id}
    # or {'action_id': action_id, 'target_id': target_id}

    db = firestore.Client()
    target_id = body['target_id']
    action = None
    if 'policy' in body:
        policy_doc = db.collection('policies').document(body['policy']).get()
        if policy_doc and policy_doc.exists:
            action = policy_doc.get('actions')[body['action_id']]
    else:
        action_doc = db.collection(common.COLLECTIONS[target_id['type']]).document(target_id['value'])\
            .collection('actions').document(body['action_id']).get()
        if action_doc and action_doc.exists:
            action = action_doc.to_dict()

            timezone = action['timezone'] if 'timezone' in action else None
            if 'maxrun' in action and 'schedule' in action:
                action['maxrun'] = action['maxrun'] - 1
                if action['maxrun'] <= 0:
                    action_doc.reference.delete()
                else:
                    task_id = schedule_task(body, action['schedule'], timezone)
                    action_doc.reference.update({'maxrun': action['maxrun'], 'task_id': task_id})
            if 'schedule' in action:
                task_id = schedule_task(body, action['schedule'], timezone)
                action_doc.reference.update({'task_id': task_id})

    if not action:
        logging.error('Missing action for %s' % json.dumps(body))
        return

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    person_ids = []
    if target_id['type'] == 'group':
        person_ids = common.get_children_ids(target_id, 'member', db)
    elif target_id['type'] == 'person':
        person_ids = [target_id]

    for person_id in person_ids:
        if not person_id or person_id['type'] != 'person':
            continue
        person_doc = db.collection('persons').document(person_id['value']).get()
        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': person_id,
            'status': 'internal',
            'tags': ['source:schedule'],
            'content_type': 'application/json',
            'content': action | {'parent': person_doc.to_dict() | {'id': person_id}}
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    return 'OK'


def schedule_task(body, schedule, timezone):
    now = datetime.datetime.utcnow()
    now = now.astimezone(pytz.timezone(timezone)) if timezone else now
    cron = croniter.croniter(schedule, now)
    next_run_time = timestamp_pb2.Timestamp()
    next_run_time.FromDatetime(cron.get_next(datetime.datetime))
    return common.schedule_task(body, tasks_v2.CloudTasksClient(), timestamp=next_run_time)