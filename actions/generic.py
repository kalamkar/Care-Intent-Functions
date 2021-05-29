import cipher
import config
import json
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1
from urllib.parse import urlencode


class Action(object):
    pass


class Message(Action):
    def __init__(self, receiver=None, sender=None, content=None):
        self.receiver = receiver
        self.sender = sender
        self.content = content

    def process(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        data = {
            'sender': self.sender,
            'receiver': self.receiver,
            'content-type': 'text/plain',
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
