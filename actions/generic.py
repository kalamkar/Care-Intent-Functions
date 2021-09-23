import abc
import croniter
import common
import config
import datetime
import dateutil.parser
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


class CreateAction(Action):
    def process(self, **kwargs):
        action_type = kwargs['action_type'] if 'action_type' in kwargs else None
        parent_id = kwargs['parent_id'] if 'parent_id' in kwargs else None
        action = kwargs['action'] if 'action' in kwargs else {}
        delay_secs = kwargs['delay_secs'] if 'delay_secs' in kwargs else None
        content = kwargs['content'] if 'content' in kwargs else None

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
        if content:
            action['params']['content'] = content

        logging.info('Creating action {}'.format(action))

        if 'schedule' in action or delay_secs:
            payload = {'action_id': action['id'], 'target_id': parent_id}
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
            logging.error('Create action is missing schedule, delay or condition')
            return

        db = firestore.Client()
        db.collection(common.COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
            .collection('actions').document(action['id']).set(action)


class UpdateResource(Action):
    def process(self, identifier=None, content=None, list_name=None, delete_field=None):
        doc_ref = firestore.Client().collection(common.COLLECTIONS[identifier['type']]).document(identifier['value'])
        if delete_field:
            logging.info('Deleting field {field} {id}'.format(field=delete_field, id=doc_ref.path))
            doc_ref.update({delete_field: firestore.DELETE_FIELD})
            return
        logging.info('Updating {id} with {data}'.format(id=doc_ref.path, data=content))
        content = json.loads(content.replace('\'', '"'), object_hook=lambda d:
            (d | {'start': dateutil.parser.parse(d['start']).astimezone(pytz.utc)}) if 'start' in d else d)
        doc_ref.update({list_name: firestore.ArrayUnion(content)} if list_name else content)


class UpdateContext(Action):
    def process(self, content=None):
        try:
            self.context_update = json.loads(content)
        except:
            logging.warning('Failed to parse json ' + content)


class UpdateData(Action):
    def process(self, source_id=None, params=None, content=None, tags=()):
        params = params if params else {}
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        if type(tags) == list:
            tags.append('source:action')
        elif type(tags) == str:
            tags = tags.split(',')

        if not params and content:
            params = json.loads(content)

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': source_id,
            'tags': tags,
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


class RunAction(Action):
    def process(self, policy=None, actions=None, delay_secs=10, target_id=None):
        if not policy or not actions or not target_id:
            logging.error('Missing action parameters')
            return
        now = datetime.datetime.utcnow()
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(now + datetime.timedelta(seconds=delay_secs))
        for action_id in [a.strip() for a in actions.split(',')]:
            payload = {'action_id': action_id, 'policy': policy, 'target_id': target_id}
            common.schedule_task(payload, tasks_v2.CloudTasksClient(), timestamp=timestamp)


class UpdateRelation(Action):
    def process(self, child_id=None, add_parent_id=None, remove_parent_id=None):
        if not child_id:
            logging.warning('Missing child_id for UpdateRelation')
            return

        if not add_parent_id and not remove_parent_id:
            logging.warning('Invalid parameters for UpdateRelation')
            return

        db = firestore.Client()
        if add_parent_id:
            data = {'id': child_id}
            if add_parent_id['type'] == 'person':
                data['proxy'] = common.get_proxy_id(add_parent_id, child_id, db, assign=True)
                if not data['proxy']:
                    logging.error('Proxy number not available, UpdateRelation failed.')
                    return
            db.collection(common.COLLECTIONS[add_parent_id['type']]).document(add_parent_id['value'])\
                .collection('members').document(child_id['type'] + ':' + child_id['value']).set(data)

        if remove_parent_id:
            db.collection(common.COLLECTIONS[remove_parent_id['type']]).document(remove_parent_id['value']) \
                .collection('members').document(child_id['type'] + ':' + child_id['value']).delete()
