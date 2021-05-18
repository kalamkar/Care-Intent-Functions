import config
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


def handle_auth(request):
    import cipher
    state = cipher.parse_auth_token(request.args.get('state'))
    print(state)
    data = {'client_id': config.DEXCOM_ID,
            'client_secret': config.DEXCOM_SECRET,
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    import requests
    response = requests.post('https://sandbox-api.dexcom.com/v2/oauth2/token', data=data)
    print(response)
