import base64
import json

from twilio.rest import Client

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'
PHONE_NUMBER = '+16692154466'


def send_sms(request):
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    if 'sender' not in message or not message['sender'] or 'value' not in message['sender']:
        message['sender'] = {'value': PHONE_NUMBER}
    print(message)
    if 'receiver' not in message or not message['receiver'] or 'value' not in message['receiver']:
        return 'ERROR'
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    client.messages.create(to=message['receiver']['value'], from_=message['sender']['value'],
                           body=message['content'])
    return 'OK'

