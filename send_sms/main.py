import base64
import json
import logging

from twilio.rest import Client

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'
PHONE_NUMBER = '+16692154466'


def send_sms(request):
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    sender = message['sender']['value'] if 'sender' in message else PHONE_NUMBER
    receiver = message['receiver']['value'] if 'receiver' in message else None
    logging.info('{} {} {}'.format(message, sender, receiver))
    if not receiver or 'content' not in message or not message['content'] or type(message['content']) != str:
        return 'ERROR'
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    try:
        client.messages.create(to=receiver, from_=sender, body=message['content'])
    except Exception as ex:
        logging.error(ex)
        return 'ERROR'
    return 'OK'
