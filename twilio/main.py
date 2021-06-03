import datetime
import json

from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


class IdType(object):
    phone = 'phone'


def twilio(request):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')

    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': IdType.phone, 'value': request.form['From']},
        'receiver': {'type': IdType.phone, 'value': request.form['To']},
        'status': 'received',
        'content_type': 'text/plain',
        'content': request.form['Body']
    }

    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return 'OK'
