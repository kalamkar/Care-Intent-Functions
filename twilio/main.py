import datetime
import dialogflow_v2 as dialogflow
import json

from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


class IdType(object):
    phone = 'phone'


def twilio(request):
    df_client = dialogflow.SessionsClient()
    session = df_client.session_path(PROJECT_ID, request.form['From'])
    text_input = dialogflow.types.TextInput(text=request.form['Body'], language_code='en-US')
    response = df_client.detect_intent(session=session, query_input=dialogflow.types.QueryInput(text=text_input))

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')

    data = {
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': IdType.phone, 'value': request.form['From']},
        'receiver': {'type': IdType.phone, 'value': request.form['To']},
        'content-type': 'text/plain',
        'type': 'intent.' + response.query_result.intent.display_name,
        'content': request.form['Body']
    }

    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return 'OK'
