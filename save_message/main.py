import base64
import cipher
import dialogflow_v2 as dialogflow
import json
import uuid

from google.cloud import firestore
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


def save_message(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    db = firestore.Client()
    message_ref = db.collection('messages').document(context.event_id)
    message['timestamp'] = context.timestamp
    message_ref.set(message)

    message['sender']['active'] = True
    person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
    persons = list(person_ref.get())
    if len(persons) == 0:
        message['sender']['active'] = True
        person_id = str(uuid.uuid4())
        db.collection('persons').document(person_id).set({
            'identifiers': [message['sender']]
        })
    else:
        person_id = persons[0].id

    df_client = dialogflow.SessionsClient()
    session = df_client.session_path(PROJECT_ID, person_id)

    text_input = dialogflow.types.TextInput(text=message['content'], language_code='en-US')
    response = df_client.detect_intent(session=session, query_input=dialogflow.types.QueryInput(text=text_input))

    if response.query_result.intent.display_name == 'connect.dexcom':
        short_code = str(uuid.uuid4())
        db.collection('urls').document(short_code).set({
            'redirect': create_dexcom_auth_url(person_id)
        })
        short_url = 'https://us-central1-careintent.cloudfunctions.net/u/' + short_code

        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, 'message')

        data = {
            'sender': message['receiver'],
            'receiver': message['sender'],
            'content-type': 'text/plain',
            'content': 'Visit {}'.format(short_url)
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')

