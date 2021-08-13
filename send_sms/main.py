import base64
import json

from google.cloud import firestore
from twilio.rest import Client

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'
PHONE_NUMBER = '+16692154466'


def send_sms(request):
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    sender = get_phone(message['sender'], PHONE_NUMBER) if 'sender' in message else PHONE_NUMBER
    receiver = get_phone(message['receiver'], None) if 'receiver' in message else None
    print(message)
    if not receiver or 'content' not in message or not message['content'] or type(message['content']) != str:
        return 'ERROR'
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    client.messages.create(to=receiver, from_=sender, body=message['content'])
    return 'OK'


def get_phone(resource, default):
    if not resource or type(resource) != dict or 'value' not in resource or 'type' not in resource:
        return default
    elif resource['type'] == 'phone':
        return resource['value']
    elif resource['type'] in ['person', 'group']:
        db = firestore.Client()
        doc = db.collection(resource['type'] + 's').document(resource['value']).get()
        ids = doc.get('identifiers')
        if not ids:
            return default
        phones = list(filter(lambda i: i['type'] == 'phone', ids))
        return phones[0] if phones else default
