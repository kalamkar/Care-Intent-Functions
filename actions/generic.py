import cipher
import config
import datetime
import json
import uuid

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from urllib.parse import urlencode


class Action(object):
    def __init__(self):
        self.output = {}

    def process(self):
        pass


class Message(Action):
    def __init__(self, receiver=None, sender=None, content=None):
        if receiver and 'type' in receiver and 'id' in receiver:
            self.receiver = receiver
        elif receiver and 'identifiers' in receiver and len(receiver['identifiers']):
            self.receiver = receiver['identifiers'][0]
        else:
            self.receiver = receiver
        self.sender = sender
        self.content = content
        super().__init__()

    def process(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': self.sender,
            'receiver': self.receiver,
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
            {'person-id': person_id, 'provider': 'dexcom', 'repeat-secs': 5 * 60})
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
            {'person-id': person_id, 'provider': 'google', 'repeat-secs': 60 * 60})
    })


PROVIDER_URLS = {'dexcom': create_dexcom_auth_url,
                 'google': create_google_auth_url}


class OAuthMessage(Message):
    def __init__(self, receiver=None, sender=None, person_id=None, provider=None):
        self.person_id = person_id
        self.provider = provider

        short_code = str(uuid.uuid4())
        db = firestore.Client()
        db.collection('urls').document(short_code).set({
            'redirect': PROVIDER_URLS[self.provider](self.person_id)
        })
        short_url = ('https://us-central1-%s.cloudfunctions.net/u/' % config.PROJECT_ID) + short_code

        super().__init__(receiver=receiver, sender=sender, content='Visit {}'.format(short_url))


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
        query = 'SELECT DISTINCT time, number FROM careintent.live.tsdatav1, UNNEST(data) ' \
                'WHERE source.id = "{source}" AND name = "{name}" ' \
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
            self.output['data'] = {'pattern': 'slope', 'rate-hour': hour_rate, 'name': self.name}


class Update(Action):
    def __init__(self, identifier=None, collection=None, content=None):
        self.identifier = identifier
        self.collection = collection
        self.content = content
        super().__init__()

    def process(self):
        db = firestore.Client()
        doc_ref = db.collection(self.collection).document(self.identifier)
        doc_ref.update(json.loads(self.content))
