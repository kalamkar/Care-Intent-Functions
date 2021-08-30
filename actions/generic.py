import croniter
import common
import config
import datetime
import json
import logging
import pytz
import requests

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

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
            msg_id = common.generate_id()
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


class CreateAction(Action):
    def __init__(self, **kwargs):
        super().__init__()
        self.action_type = kwargs['action_type'] if 'action_type' in kwargs else None
        self.parent_id = kwargs['parent_id'] if 'parent_id' in kwargs else None
        self.action = kwargs['action'] if 'action' in kwargs else None
        self.delay_secs = kwargs['delay_secs'] if 'delay_secs' in kwargs else None

    def process(self):
        if not self.parent_id or not self.action_type:
            logging.warning('Missing parent id or action type for delayed action')
            return
        action = self.action | {'id': common.generate_id(), 'type': self.action_type}
        for filtered_param in ['condition', 'parent', 'action', 'action_type', 'parent_id', 'delay_secs']:
            if filtered_param in action:
                del action[filtered_param]
        for top_param in ['priority', 'condition', 'schedule', 'timezone', 'hold_secs']:
            if top_param in action['params']:
                action[top_param] = action['params'][top_param]
                del action['params'][top_param]

        if 'schedule' in action or self.delay_secs:
            payload = {'action_id': action['id'],
                       'group_id' if self.parent_id['type'] == 'group' else 'person_id': self.parent_id['value']}
            now = datetime.datetime.utcnow()
            if self.delay_secs:
                start_time = now + datetime.timedelta(seconds=self.delay_secs)
            else:
                now = now.astimezone(pytz.timezone(action['timezone'])) if 'timezone' in action else now
                cron = croniter.croniter(action['schedule'], now)
                start_time = cron.get_next(datetime.datetime)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(start_time)
            action['task_id'] = common.schedule_task(payload, tasks_v2.CloudTasksClient(), timestamp=timestamp)
        elif 'condition' not in action:
            logging.warning('Create action is missing schedule, delay or condition')
            return

        db = firestore.Client()
        db.collection(common.COLLECTIONS[self.parent_id['type']]).document(self.parent_id['value'])\
            .collection('actions').document(action['id']).set(action)


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


class UpdateData(Action):
    def __init__(self, person_id=None, params=None, content=None):
        self.person_id = person_id
        self.params = params if params else {}
        self.content = content
        super().__init__()

    def process(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        if not self.params and self.content:
            self.params = json.loads(self.content)

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
