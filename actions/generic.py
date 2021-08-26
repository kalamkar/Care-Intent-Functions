import base64

import common
import config
import datetime
import json
import logging
import requests
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content
from sendgrid.helpers.mail import Mail


class Action(object):
    def __init__(self):
        self.context_update = {}
        self.action_update = {}

    def process(self):
        pass


class Message(Action):
    def __init__(self, receiver=None, sender=None, content=None, queue=False, tags=None):
        self.receiver = receiver
        self.sender = sender
        self.content = content
        self.queue = queue
        self.tags = ['source:action']
        if type(tags) == list:
            self.tags.extend(tags)
        elif type(tags) == str:
            self.tags.extend(tags.split(','))
        super().__init__()

    def process(self):
        if self.queue:
            db = firestore.Client()
            msg_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
            msg = db.collection('persons').document(self.receiver['value']).collection('messages').document(msg_id)
            msg.set({
                'time': datetime.datetime.utcnow().isoformat(),
                'sender': self.sender,
                'receiver': self.receiver,
                'tags': self.tags,
                'content_type': 'text/plain',
                'content': self.content
            })
            return

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': self.sender,
            'receiver': self.receiver,
            'status': 'sent',
            'tags': self.tags,
            'content_type': 'text/plain',
            'content': self.content
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')


class UpdateResource(Action):
    def __init__(self, identifier=None, content=None, list_name=None):
        self.identifier = identifier
        self.content = content
        self.list_name = list_name
        super().__init__()

    def process(self):
        db = firestore.Client()
        collection = common.COLLECTIONS[self.identifier['type']]
        doc_ref = db.collection(collection).document(self.identifier['value'])
        logging.info('Updating {collection}/{id} with {data}'.format(collection=collection, id=self.identifier['value'],
                                                                     data=self.content))
        content = json.loads(self.content.replace('\'', '"'))
        doc_ref.update({self.list_name: firestore.ArrayUnion(content)} if self.list_name else content)


class UpdateContext(Action):
    def __init__(self, content=None):
        self.content = content
        super().__init__()

    def process(self):
        try:
            self.context_update = json.loads(self.content)
        except:
            logging.warning('Failed to parse json ' + self.content)


class DataExtract(Action):
    def __init__(self, person_id=None, params=None):
        self.person_id = person_id
        self.params = params if params else {}
        super().__init__()

    def process(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': {'type': 'person', 'value': self.person_id},
            'tags': ['self'],
            'data': []
        }
        for name, value in self.params.items():
            if type(value) in [int, float]:
                row['data'].append({'name': name, 'number': value})
            elif type(value) == str:
                row['data'].append({'name': name, 'value': value})
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


class Webhook(Action):
    def __init__(self, url=None, auth=None, email=None, content=None, content_type='application/json'):
        self.url = url
        self.auth = auth
        self.email = email
        self.content = content
        self.content_type = content_type
        super().__init__()

    def process(self):
        if not self.url and not self.email:
            logging.error('Missing url and email')
            return

        headers = {'Content-Type': self.content_type}
        if self.auth:
            headers['Authorization'] = 'Bearer ' + self.auth
        if self.url:
            requests.post(self.url, self.content, headers=headers)

        if self.email:
            message = Mail(from_email='support@careintent.com', to_emails=self.email,
                           subject='Webhook', plain_text_content=Content('text/plain', self.content))
            SendGridAPIClient('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').send(message)
