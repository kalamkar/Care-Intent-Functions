import abc
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


class Action(abc.ABC):
    def __init__(self):
        self.context_update = {}
        self.action_update = {}

    @abc.abstractmethod
    def process(self):
        pass


class Message(Action):
    def process(self, receiver=None, sender=None, content=None, queue=False, tags=None):
        if type(tags) == list:
            tags.append('source:action')
        elif type(tags) == str:
            tags = tags.split(',') + ['source:action']

        db = firestore.Client()
        if queue:
            msg_id = common.generate_id()
            msg = db.collection('persons').document(receiver['value']).collection('messages').document(msg_id)
            msg.set({
                'time': datetime.datetime.utcnow().isoformat(),
                'sender': sender,
                'receiver': receiver,
                'tags': tags,
                'content_type': 'text/plain',
                'content': content
            })
            return

        sender = common.get_identifier(sender, 'phone', db,
                                       {'type': 'phone', 'value': config.PHONE_NUMBER}, ['group'])
        receiver = common.get_identifier(receiver, 'phone', db)
        if not receiver:
            logging.warning('Missing receiver for message action')
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


class CreateAction(Action):
    def process(self, **kwargs):
        action_type = kwargs['action_type'] if 'action_type' in kwargs else None
        parent_id = kwargs['parent_id'] if 'parent_id' in kwargs else None
        action = kwargs['action'] if 'action' in kwargs else None
        delay_secs = kwargs['delay_secs'] if 'delay_secs' in kwargs else None

        if not parent_id or not action_type:
            logging.warning('Missing parent id or action type for delayed action')
            return
        action = action | {'id': common.generate_id(), 'type': action_type}
        if 'condition' in action:
            del action['condition']
        if 'parent' in action:
            del action['parent']
        for filtered_param in ['action', 'action_type', 'parent_id', 'delay_secs']:
            if filtered_param in action['params']:
                del action['params'][filtered_param]
        for top_param in ['priority', 'condition', 'schedule', 'timezone', 'hold_secs', 'maxrun']:
            if top_param in action['params']:
                action[top_param] = action['params'][top_param]
                del action['params'][top_param]

        if 'schedule' in action or delay_secs:
            payload = {'action_id': action['id'], 'parent_id': parent_id}
            now = datetime.datetime.utcnow()
            if delay_secs:
                start_time = now + datetime.timedelta(seconds=delay_secs)
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
        db.collection(common.COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
            .collection('actions').document(action['id']).set(action)


class UpdateResource(Action):
    def process(self, identifier=None, content=None, list_name=None):
        db = firestore.Client()
        collection = common.COLLECTIONS[identifier['type']]
        doc_ref = db.collection(collection).document(identifier['value'])
        logging.info('Updating {collection}/{id} with {data}'.format(collection=collection, id=identifier['value'],
                                                                     data=content))
        content = json.loads(content.replace('\'', '"'))
        doc_ref.update({list_name: firestore.ArrayUnion(content)} if list_name else content)


class UpdateContext(Action):
    def process(self, content=None):
        try:
            self.context_update = json.loads(content)
        except:
            logging.warning('Failed to parse json ' + content)


class UpdateData(Action):
    def process(self, person_id=None, params=None, content=None):
        params = params if params else {}
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        if not params and content:
            params = json.loads(content)

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': {'type': 'person', 'value': person_id},
            'tags': ['self'],
            'data': []
        }
        for name, value in params.items():
            if type(value) in [int, float]:
                row['data'].append({'name': name, 'number': value})
            elif type(value) == str:
                row['data'].append({'name': name, 'value': value})
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


class Webhook(Action):
    def process(self, url=None, auth=None, email=None, content=None, content_type='application/json'):
        if not url and not email:
            logging.error('Missing url and email')
            return

        headers = {'Content-Type': content_type}
        if auth:
            headers['Authorization'] = 'Bearer ' + auth
        if url:
            requests.post(url, content, headers=headers)

        if email:
            message = Mail(from_email='support@careintent.com', to_emails=email,
                           subject='Webhook', plain_text_content=Content('text/plain', content))
            SendGridAPIClient(config.SENDGRID_TOKEN).send(message)


class DelayRun(Action):
    def process(self, parent_id=None, action_id=None, delay_secs=None):
        payload = {'action_id': action_id, 'parent_id': parent_id}
        now = datetime.datetime.utcnow()
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(now + datetime.timedelta(seconds=delay_secs))
        common.schedule_task(payload, tasks_v2.CloudTasksClient(), timestamp=timestamp)
