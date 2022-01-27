import base64
import common
import config
import croniter
import datetime
import json
import logging
import pytz

import assessment
import barriers
import diary
import education
import feedback
import chitchat

from context import Context
from google.cloud import pubsub_v1
from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

CONVERSATIONS = [feedback, diary, education, barriers, assessment, chitchat]


def main(event, metadata):
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    db = firestore.Client()
    context = Context()
    context.set(channel_name, message)
    status = context.get('message.status')
    tags = context.get('message.tags') or []
    person_update = {}
    if channel_name == 'message' and 'sender' in message and status == 'received' and 'proxy' not in tags:
        context.set('person', common.get_resource(message['sender'], db))
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data', {data['name']: data['number'] if 'number' in data else data['value']})
        context.set('person', common.get_resource(message['source'], db))
    else:
        return

    if 'conversations' not in context.get('person') or 'tasks' not in context.get('person'):
        logging.info('Task based conversations not enabled for this person.')
        return

    logging.info(message)

    person = context.get('person')
    if message['status'] == 'engage' or 'task_id' not in person:
        person_update['task_id'] = schedule_next_task(person)

    replies = []
    conversations = [(get_conversation_module(conv['type']), conv) for conv in person['conversations']]
    selected_index = -1
    conversation = chitchat.Conversation({}, context)
    for selected_index, conversation_module, conversation_config in enumerate(conversations):
        conversation = conversation_module.Conversation(conversation_config, context)
        if conversation.can_process():
            break

    logging.info('%s conversation about %s' %
                 (conversation.__module__, conversation.config['task_id'] if 'task_id' in conversation.config else ''))
    conversation.process()
    replies.append(conversation.reply)
    if 0 <= selected_index < len(person['conversations']):
        person['conversations'][selected_index]['last_run_time'] = datetime.datetime.utcnow()
        person['conversations'][selected_index]['last_message_type'] = conversation.last_message_type

    transfers = list(filter(lambda conv: conv[1]['type'] == conversation.transfer_type, conversations))
    if transfers:
        conversation_module, conversation_config = transfers[0]
        conversation = conversation_module.Conversation(conversation_config, context)
        conversation.process()
        replies.append(conversation.reply)
        conversation.config['last_run_time'] = datetime.datetime.utcnow()
        conversation.config['last_message_type'] = conversation.last_message_type

    if replies:
        send_message(message['receiver'], message['sender'], ' '.join(filter(lambda r: r.strip(), replies)))
    else:
        logging.warning('No reply generated')

    person_update['conversations'] = person['conversations']
    db.collection('persons').document(person['id']['value']).update(person_update)


def schedule_next_task(person):
    now = datetime.datetime.utcnow()
    now = now.astimezone(pytz.timezone(person['timezone'])) if 'timezone' in person else now
    timings = []
    for identifier, task in person['tasks'].items():
        if 'system' in task and task['system']:
            timings.append(croniter.croniter(task['schedule'], now).get_next(datetime.datetime))

    timings.sort()
    next_run_time = timestamp_pb2.Timestamp()
    next_run_time.FromDatetime(timings[0] if timings else (now + datetime.timedelta(hours=24)))
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': person['id'],
        'status': 'engage',
        'tags': ['source:schedule'],
        'content_type': 'application/json',
        'content': {}
    }
    return common.schedule_task(data, tasks_v2.CloudTasksClient(), timestamp=next_run_time)


def get_conversation_module(conversation_type):
    for conv in CONVERSATIONS:
        if conv.__name__ == conversation_type:
            return conv
    return chitchat


def send_message(sender, receiver, content, tags=()):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': sender,
        'receiver': receiver,
        'status': 'sent',
        'tags': tags,
        'content_type': 'text/plain',
        'content': content
    }
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')
