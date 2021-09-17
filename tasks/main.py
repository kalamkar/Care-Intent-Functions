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

    db = firestore.Client()
    parent_id = body['parent_id']
    action_doc = db.collection(common.COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
        .collection('actions').document(body['action_id']).get()

    if not action_doc or not action_doc.exists:
        logging.error('Missing action for %s' % json.dumps(body))
        return

    action = action_doc.to_dict()
    if 'maxrun' in action:
        action['maxrun'] = action['maxrun'] - 1
        if action['maxrun'] <= 0:
            action_doc.reference.delete()
            action_doc = None

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
    if 'person_id' in body:
        member_ids = [body['person_id']]
    elif parent_id['type'] == 'group':
        member_ids = common.get_children_ids(parent_id, 'member', db)
    elif parent_id['type'] == 'person':
        member_ids = [parent_id]

    for member in member_ids:
        if not member or member['type'] != 'person':
            continue
        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': member,
            'status': 'internal',
            'tags': ['source:schedule'],
            'content_type': 'application/json',
            'content': body
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    return 'OK'
