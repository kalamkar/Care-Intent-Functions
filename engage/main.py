import base64
import sys

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
import followup
import chitchat

from messages import DATA as messages

from context import Context
from google.cloud import pubsub_v1
from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

CONVERSATIONS = [followup, diary, education, barriers, assessment, chitchat]


def main(event, metadata):
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    db = firestore.Client()
    context = Context()
    context.set(channel_name, message)
    status = context.get('message.status')
    tags = context.get('message.tags') or []
    person_update = {}
    if channel_name == 'message' and 'sender' in message and status in ['received', 'engage'] and 'proxy' not in tags:
        context.set('person', common.get_resource(message['receiver' if 'schedule_only' in tags else 'sender'], db))
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data', {data['name']: data['number'] if 'number' in data else data['value']})
        context.set('person', common.get_resource(message['source'], db))
    else:
        return

    if 'conversations' not in context.get('person') or 'tasks' not in context.get('person'):
        logging.info('Task based conversations not enabled for this person.')
        return

    logging.info('Conversations for context {}'.format(json.dumps(context.data, default=str)))

    person = context.get('person')

    if 'schedule_only' in tags:
        person_update['task_id'] = schedule_next_task(person)
        db.collection('persons').document(person['id']['value']).update(person_update)
        logging.info('Skipping reply for schedule only message')
        return

    # In case person has not consented
    if not context.get('person.consent_time'):
        # Outgoing message and opted out person
        if message['content_type'] == 'application/json' and context.get('person.opted_out'):
            logging.warning('Person has opted out, remove them from scheduled message.')
        # Incoming message and person said yes
        elif message['content_type'] != 'application/json' and \
                context.get('message.nlp.action') == 'smalltalk.confirmation.yes':
            person_update['consent_time'] = datetime.datetime.utcnow()
            person_update['opted_out'] = False
            db.collection('persons').document(person['id']['value']).update(person_update)
            send_message(message['receiver'], message['sender'], context.render(messages['welcome']), db)
        # Incoming message and person said no
        elif message['content_type'] != 'application/json' and \
                context.get('message.nlp.action') == 'smalltalk.confirmation.no':
            person_update['opted_out'] = True
            db.collection('persons').document(person['id']['value']).update(person_update)
        else:
            send_message(message['receiver'] if 'receiver' in message else None,
                         message['sender'], context.render(messages['consent']), db)
        return

    replies = []
    conversations = [(get_conversation_module(conv['type']), conv) for conv in person['conversations']]
    selected_index = -1
    conversation = None
    for selected_index, conv in enumerate(conversations):
        conversation_module, conversation_config = conv
        enabled_conversation = conversation_module.Conversation(conversation_config, context)
        if enabled_conversation.can_process():
            conversation = enabled_conversation
            break

    if not conversation:
        person_update['task_id'] = schedule_next_task(person)
        db.collection('persons').document(person['id']['value']).update(person_update)
        logging.warning('No conversation found to reply')
        return

    try:
        logging.info('%s conversation with config %s' % (conversation.__module__, conversation.config))
        conversation.process()
        replies.append(conversation.get_reply())
        if 0 <= selected_index < len(person['conversations']):
            person['conversations'][selected_index]['last_run_time'] = datetime.datetime.utcnow()
            if not conversation.skip_message_id_update:
                person_update['last_message_id'] = conversation.__module__ + '.' + '.'.join(conversation.message_id)

        transfers = list(filter(lambda conv: conv[1]['type'] == conversation.transfer_type, conversations))
        if transfers:
            conversation_module, conversation_config = transfers[0]
            logging.info('Transferred to %s' % conversation_module)
            conversation = conversation_module.Conversation(conversation_config, context)
            conversation.process()
            replies.append(conversation.get_reply())
            conversation.config['last_run_time'] = datetime.datetime.utcnow()
            if not conversation.skip_message_id_update:
                person_update['last_message_id'] = conversation.__module__ + '.' + '.'.join(conversation.message_id)

        reply = ' '.join(filter(lambda r: r.strip(), replies)).strip()
        if status == 'engage' and reply and 'sender' in message:
            send_message(None, message['sender'], reply, db)
        elif reply and 'receiver' in message and 'sender' in message:
            send_message(message['receiver'], message['sender'], reply, db)
        else:
            logging.warning('No reply generated')
    except:
        logging.error(sys.exc_info()[1])
        logging.error(sys.exc_info()[2])

    person_update['task_id'] = schedule_next_task(person)
    person_update['conversations'] = person['conversations']
    person_update['session.last_sent_time'] = datetime.datetime.utcnow().astimezone(pytz.utc)
    db.collection('persons').document(person['id']['value']).update(person_update)


def schedule_next_task(person):
    now = datetime.datetime.utcnow()
    now = now.astimezone(pytz.timezone(person['timezone'])) if 'timezone' in person else now
    timings = []
    for conversation in person['conversations']:
        if 'schedule' in conversation:
            timings.append((croniter.croniter(conversation['schedule'], now).get_next(datetime.datetime), conversation))
        if 'repeat' in conversation and conversation['repeat'] and 'repeat_question' in conversation:
            repeat_secs = common.get_duration_secs(conversation['repeat'])
            timings.append((now + datetime.timedelta(seconds=repeat_secs), conversation))

    timings.sort(key=lambda t: t[0])
    earliest_time = timings[0][0] if timings else (now + datetime.timedelta(hours=24))
    next_run_time = timestamp_pb2.Timestamp()
    next_run_time.FromDatetime(earliest_time)
    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': person['id'],
        'status': 'engage',
        'tags': ['source:schedule'],
        'content_type': 'application/json',
        'content': {'conversation': timings[0][1] if timings else None}
    }
    client = tasks_v2.CloudTasksClient()
    current_task = common.get_task(person['task_id'], client, queue_name='engage') if 'task_id' in person else None
    if not current_task:
        return common.schedule_task(data, client, timestamp=next_run_time, queue_name='engage')
    if current_task.schedule_time <= earliest_time:
        return current_task.name
    logging.info("Deleting task {}".format(current_task.name))
    client.delete_task(name=current_task.name)
    return common.schedule_task(data, client, timestamp=next_run_time, queue_name='engage')


def get_conversation_module(conversation_type):
    for conv in CONVERSATIONS:
        if conv.__name__ == conversation_type:
            return conv
    return chitchat


def send_message(sender, receiver, content, db, tags=()):
    sender = common.get_identifier(sender, 'phone', db,
                                   {'type': 'phone', 'value': config.PHONE_NUMBER}, ['group'])
    receiver = common.get_identifier(receiver, 'phone', db)
    if not receiver:
        logging.warning('Missing receiver for to send message')
        return
    if sender == receiver or receiver['value'] in [config.PHONE_NUMBER] + config.PROXY_PHONE_NUMBERS:
        logging.error('Sending message to system phone numbers to {r} from {s}'.format(r=receiver, s=sender))
        return
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
