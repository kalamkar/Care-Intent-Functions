import base64
import json

from google.cloud import firestore
from generic import DexcomAuth


def process(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    message['sender']['active'] = True
    db = firestore.Client()
    person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
    persons = list(person_ref.get())
    if len(persons) == 0:
        return 500, 'Not ready'

    if 'type' in message and message['type'] == 'intent.connect.dexcom':
        DexcomAuth(receiver=message['sender'], sender=message['receiver'],
                   person_id=persons[0].id).process()
