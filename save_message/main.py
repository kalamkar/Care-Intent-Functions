import base64
import json
import uuid

from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


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
