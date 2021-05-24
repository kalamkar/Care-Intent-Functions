import base64
import config
import json

from twilio.rest import Client

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def send_sms(request):
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    client = Client(config.TWILIO_ACCOUNT_SID, config.TWILIO_AUTH_TOKEN)
    client.messages.create(to=message['receiver']['value'], from_=message['sender']['value'],
                           body=message['content'])
    print(message)
    return 'OK'

