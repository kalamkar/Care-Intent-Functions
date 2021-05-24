import base64
import cipher
import json
import uuid

from google.cloud import firestore
from google.cloud import pubsub_v1
from urllib.parse import urlencode

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
DEXCOM_ID = 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg'


def create_dexcom_auth_url(person_id):
    return 'https://sandbox-api.dexcom.com/v2/oauth2/login?' + urlencode({
        'client_id': DEXCOM_ID,
        'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth',
        'response_type': 'code',
        'scope': 'offline_access',
        'state': cipher.create_auth_token({'person-id': person_id, 'provider': 'dexcom'})
    })


def process(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    if 'type' in message and message['type'] == 'intent.connect.dexcom':
        db = firestore.Client()
        message['sender']['active'] = True
        person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
        persons = list(person_ref.get())
        if len(persons) == 0:
            return 500, 'Not ready'

        short_code = str(uuid.uuid4())
        db.collection('urls').document(short_code).set({
            'redirect': create_dexcom_auth_url(persons[0].id)
        })
        short_url = 'https://us-central1-careintent.cloudfunctions.net/u/' + short_code

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, 'message')

        data = {
            'sender': message['receiver'],
            'receiver': message['sender'],
            'content-type': 'text/plain',
            'content': 'Visit {}'.format(short_url)
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')

