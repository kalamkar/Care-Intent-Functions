import base64
import cipher
import config
import datetime
import json
import logging
import requests
import uuid

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from urllib.parse import urlencode

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
            'tags': self.tags,
            'content_type': 'text/plain',
            'content': self.content
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')


def create_dexcom_auth_url(person_id):
    return 'https://sandbox-api.dexcom.com/v2/oauth2/login?' + urlencode({
        'client_id': config.DEXCOM_ID,
        'redirect_uri': 'https://us-central1-%s.cloudfunctions.net/auth' % config.PROJECT_ID,
        'response_type': 'code',
        'scope': 'offline_access',
        'state': cipher.create_auth_token(
            {'person_id': person_id, 'action_id': 'dexcom', 'schedule': '0-55/5 * * * *'})
    })


def create_google_auth_url(person_id):
    return 'https://accounts.google.com/o/oauth2/v2/auth?' + urlencode({
        'prompt': 'consent',
        'response_type': 'code',
        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
        'scope': 'https://www.googleapis.com/auth/fitness.activity.read',
        'access_type': 'offline',
        'redirect_uri': 'https://us-central1-%s.cloudfunctions.net/auth' % config.PROJECT_ID,
        'state': cipher.create_auth_token(
            {'person_id': person_id, 'action_id': 'google', 'schedule': '0 * * * *'})
    })


PROVIDER_URLS = {'dexcom': create_dexcom_auth_url,
                 'google': create_google_auth_url}


class OAuth(Action):
    def __init__(self, person_id=None, provider=None):
        self.person_id = person_id
        self.provider = provider
        super().__init__()

    def process(self):
        short_code = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
        db = firestore.Client()
        db.collection('urls').document(short_code).set({
            'redirect': PROVIDER_URLS[self.provider](self.person_id)
        })
        self.context_update['oauth'] = {
            'url': ('https://us-central1-%s.cloudfunctions.net/u/' % config.PROJECT_ID) + short_code
        }


class SimplePatternCheck(Action):
    def __init__(self, person_id=None, name=None, seconds=None, min_threshold=None, max_threshold=None):
        self.person_id = person_id
        self.name = name
        self.seconds = seconds
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        super().__init__()

    def process(self):
        client = bigquery.Client()
        query = 'SELECT DISTINCT time, number FROM careintent.live.tsdata, UNNEST(data) ' \
                'WHERE source.value = "{source}" AND name = "{name}" ' \
                'AND time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {seconds} second) ' \
                'ORDER BY time'.\
            format(source=self.person_id, name=self.name, seconds=self.seconds)
        data = []
        for row in client.query(query):
            data.append((row['time'], row['number']))

        if len(data) < 2:
            return
        seconds = (data[-1][0] - data[0][0]).total_seconds()
        hour_rate = (data[-1][1] - data[0][1]) * (60 * 60) / (seconds if seconds else 1)
        if (self.min_threshold and hour_rate < self.min_threshold) or\
           (self.max_threshold and hour_rate > self.max_threshold):
            self.context_update['data'] = {'pattern': 'slope', 'rate-hour': hour_rate, 'name': self.name}


class Update(Action):
    def __init__(self, identifier=None, content=None, list_name=None):
        self.identifier = identifier
        self.content = content
        self.list_name = list_name
        super().__init__()

    def process(self):
        db = firestore.Client()
        collection = self.identifier['type'] + 's'
        doc_ref = db.collection(collection).document(self.identifier['value'])
        logging.info('Updating {collection}/{id} with {data}'.format(collection=collection, id=self.identifier['value'],
                                                                     data=self.content))
        content = json.loads(self.content.replace('\'', '"'))
        doc_ref.update({self.list_name: firestore.ArrayUnion(content)} if self.list_name else content)


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
    def __init__(self, url=None, name=None, time=None, text=None, data=None, auth=None):
        self.url = url
        self.name = name
        self.time = time
        if not self.time:
            self.time = datetime.datetime.utcnow().isoformat()
        if type(self.time) == datetime.datetime:
            self.time = self.time.isoformat()
        self.text = text
        self.data = data
        if self.data and type(self.data) == str:
            try:
                self.data = json.loads(self.data)
            except:
                pass
        self.auth = auth
        super().__init__()

    def process(self):
        if not self.url or not self.name:
            logging.error('Missing url or name')
            return

        headers = {'Content-Type': 'application/json'}
        if self.auth:
            headers['Authorization'] = 'Bearer ' + self.auth
        body = {
            'time': self.time,
            'name': self.name,
            'text': self.text,
            'data': self.data
        }
        # requests.post(self.url, body, headers=headers)
        message = Mail(from_email='support@careintent.com', to_emails='support@careintent.com',
                       subject='Webhook: ' + self.name, plain_text_content=Content('text/plain', json.dumps(body)))
        SendGridAPIClient('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').send(message)
