import base64
import common
import config
import json
import logging

from twilio.rest import Client

from google.cloud import firestore
import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content
from sendgrid.helpers.mail import Mail

TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'
SENDGRID_TOKEN = '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'


def main(request):
    db = firestore.Client()
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    if 'receiver' not in message or 'type' not in message['receiver'] or 'value' not in message['receiver']:
        logging.warning('Missing or invalid receiver in ' + str(message))
        return 'ERROR'
    if 'content' not in message or not message['content'] or type(message['content']) != str:
        logging.warning('Missing or invalid content in ' + str(message))
        return 'ERROR'

    channel = message['receiver']['type']
    sender = common.get_identifier(message['sender'], channel, db, resource_types=['group']) \
        if 'sender' in message else None
    sender = sender['value'] if sender else None
    receiver = message['receiver']['value']
    logging.info('{} {} {}'.format(message, sender, receiver))
    try:
        if channel == 'phone':
            send_sms(message['content'], sender, receiver)
        elif channel == 'email':
            subject = message['subject'] if 'subject' in message else 'Message'
            send_email(message['content'], subject, sender, receiver)
    except Exception as ex:
        logging.error(ex)
        return 'ERROR'
    return 'OK'


def send_sms(content, sender, receiver):
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    client.messages.create(to=receiver, from_=sender or config.PHONE_NUMBER, body=content.replace('\\n', '\n'))


def send_email(content, subject, sender, receiver):
    message = Mail(from_email=sender or config.EMAIL_ADDRESS, to_emails=receiver, subject=subject,
                   plain_text_content=Content('text/plain', content))
    SendGridAPIClient(SENDGRID_TOKEN).send(message)
