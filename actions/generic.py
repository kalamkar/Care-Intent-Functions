import cipher
import config
import json
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1
from urllib.parse import urlencode


class Action(object):
    pass


def create_dexcom_auth_url(person_id):
    return 'https://sandbox-api.dexcom.com/v2/oauth2/login?' + urlencode({
        'client_id': config.DEXCOM_ID,
        'redirect_uri': 'https://us-central1-%s.cloudfunctions.net/auth' % config.PROJECT_ID,
        'response_type': 'code',
        'scope': 'offline_access',
        'state': cipher.create_auth_token({'person-id': person_id, 'provider': 'dexcom'})
    })


def create_google_auth_url(_):
    return 'https://accounts.google.com/o/oauth2/v2/auth?' + urlencode({
        'prompt': 'consent',
        'response_type': 'code',
        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
        'scope': 'https://www.googleapis.com/auth/fitness.activity.read',
        'access_type': 'offline',
        'redirect_uri': 'https://us-central1-%s.cloudfunctions.net/auth' % config.PROJECT_ID
    })


PROVIDER_URLS = {'dexcom': create_dexcom_auth_url,
                 'google': create_google_auth_url}


class OAuthMessage(Action):
    def __init__(self, receiver=None, sender=None, person_id=None, provider=None):
        self.receiver = receiver
        self.sender = sender
        self.person_id = person_id
        self.provider = provider

    def process(self):
        short_code = str(uuid.uuid4())
        db = firestore.Client()
        db.collection('urls').document(short_code).set({
            'redirect': PROVIDER_URLS[self.provider](self.person_id)
        })
        short_url = ('https://us-central1-%s.cloudfunctions.net/u/' % config.PROJECT_ID) + short_code

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        data = {
            'sender': self.sender,
            'receiver': self.receiver,
            'content-type': 'text/plain',
            'content': 'Visit {}'.format(short_url)
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')
