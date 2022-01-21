import base64
import common
import config
import datetime
import json
import logging

import reminder
import activity_logging
import device_data_logging
import outcome_reporting
import knowledge_base
import side_effect_check_in
import soft_data_check_in
import content_delivery
import barrier_identification
import survey_assessment
import positive_reinforcement
import generic_form_filling
import smalltalk

from context import Context
from google.cloud import pubsub_v1
from google.cloud import firestore

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

CONVERSATIONS = [reminder, device_data_logging, activity_logging, outcome_reporting, knowledge_base,
                 side_effect_check_in, soft_data_check_in, content_delivery, barrier_identification, survey_assessment,
                 generic_form_filling, positive_reinforcement, smalltalk]


def main(event, metadata):
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    logging.info(message)

    db = firestore.Client()
    context = Context()
    context.set(channel_name, message)
    status = context.get('message.status')
    tags = context.get('message.tags') or []
    if channel_name == 'message' and 'sender' in message and status == 'received' and 'proxy' not in tags:
        context.set('person', common.get_resource(message['sender'], db))
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data', {data['name']: data['number'] if 'number' in data else data['value']})
        context.set('person', common.get_resource(message['source'], db))

    if 'conversations' not in context.get('sender'):
        logging.info('Engage conversations not enabled for this sender.')
        return

    replies = []
    for _ in range(3):
        sender = context.get('sender')
        conversations = [(get_conversation_module(conv['id']['type']), conv) for conv in sender['conversations']]
        if 'current_conversation_id' in sender:
            conversations = filter(lambda conv: conv[1]['id'] == sender['current_conversation_id'], conversations)

        for conversation_module, conversation_config in conversations:
            if not conversation_module:
                continue
            conversation = conversation_module.Conversation(conversation_config, context)
            conversation.process()

            if conversation.reply:
                replies.append(conversation.reply)
                if 'current_conversation_id' not in sender:
                    context.set('sender', {'current_conversation_id': conversation_config['id']})

            if not conversation.transfer_type:
                continue
            transfers = list(filter(lambda conv: conv['id']['type'] == conversation.transfer_type,
                                    sender['conversations']))
            if not transfers:
                continue
            context.set('sender', {'current_conversation_id': transfers[0]['id']})
            break

    if replies:
        send_message(context.get('receiver'), context.get('sender'), ' '.join(replies))
    else:
        logging.warning('No reply generated')


def get_conversation_module(conversation_type):
    for conv in CONVERSATIONS:
        if conv.__name__ == conversation_type:
            return conv
    return None


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
