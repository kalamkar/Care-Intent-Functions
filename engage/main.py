import base64
import common
import config
import datetime
import json
import logging

import smart_reminder
import activity_logging
import device_data_logging
import outcome_reporting
import knowledge_base
import hard_reminder
import side_effect_check_in
import soft_data_check_in
import content_delivery
import barrier_identification
import survey_assessment
import positive_reinforcement
import generic_form_filling

from context import Context
from google.cloud import pubsub_v1
from google.cloud import firestore

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

CONVERSATIONS = [smart_reminder, device_data_logging, activity_logging, outcome_reporting, knowledge_base,
                 hard_reminder, side_effect_check_in, soft_data_check_in, content_delivery, barrier_identification,
                 survey_assessment, generic_form_filling, positive_reinforcement]


def main(event, metadata):
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    logging.info(message)

    db = firestore.Client()
    context = Context()
    context.set(channel_name, message)
    if channel_name == 'message':
        if 'sender' in message:
            context.set('sender', common.get_resource(message['sender'], db))
        if 'receiver' in message:
            context.set('receiver', common.get_resource(message['receiver'], db))
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data', {data['name']: data['number'] if 'number' in data else data['value']})
        context.set('sender', common.get_resource(message['source'], db))

    if 'conversations' not in context.get('sender'):
        logging.info('Engage conversations not enabled for this sender.')
        return

    sender = context.get('sender')
    conversations = {conv.ID: conv for conv in filter(lambda c: c.ID in sender['conversations'], CONVERSATIONS)}
    if 'current' in sender['conversations'] and sender['conversations']['current'] in conversations:
        conversations[sender['conversations']['current']].handle(context)
    else:
        for conv in conversations:
            conv.handle(context)

    if context.get('replies'):
        send_message(sender, context.get('receiver'), ' '.join(context.get('replies')))


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
