import json
import os

PROJECT_ID = os.environ.get('GCP_PROJECT')


class IdType(object):
    phone = 'phone'


def on_sms(request):
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('careintent', 'message')

    data = {
        'sender': {'type': IdType.phone, 'value': request.form['From']},
        'receiver': {'type': IdType.phone, 'value': request.form['To']},
        'content-type': 'text/plain',
        'content': request.form['Body']
    }

    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return 'OK'


def save_message(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    import base64
    from google.cloud import firestore
    db = firestore.Client()
    data = base64.b64decode(event['data']).decode('utf-8')

    message_ref = db.collection('messages').document(context.event_id)
    message = json.loads(data)
    message['timestamp'] = context.timestamp
    message_ref.set(message)


def on_fs_message_write(event, context):
    """Triggered by a change to a Firestore document.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    resource_string = context.resource
    # print out the resource string that triggered the function
    print(f"{context} {resource_string}.")
    # now print out the entire event object
    print(str(event))


def resolve_intent(event, context):
    """Triggered by a change to a Firestore document.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    resource_string = context.resource
    # print out the resource string that triggered the function
    print(f"Function triggered by change to: {resource_string}.")
    # now print out the entire event object
    print(str(event))

    # import dialogflow_v2 as dialogflow
    # df_client = dialogflow.SessionsClient()
    # session = df_client.session_path(PROJECT_ID, phone_number)
    #
    # text_input = dialogflow.types.TextInput(text=text, language_code='en-US')
    # response = df_client.detect_intent(session=session, query_input=dialogflow.types.QueryInput(text=text_input))


ALLOW_HEADERS = 'Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Authorization, ' \
                'Cookie, Access-Control-Request-Method, Access-Control-Request-Headers, ' \
                'Access-Control-Allow-Credentials, Cache-Control, Pragma, Expires'


def make_response(request, methods='GET'):
    import flask
    response = flask.make_response()
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'

    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = methods
        response.headers['Access-Control-Allow-Headers'] = ALLOW_HEADERS
        response.status_code = 204

    return response
