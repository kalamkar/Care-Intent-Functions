import config
import json
import os

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


class IdType(object):
    phone = 'phone'


def twilio(request):
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')

    data = {
        'sender': {'type': IdType.phone, 'value': request.form['From']},
        'receiver': {'type': IdType.phone, 'value': request.form['To']},
        'content-type': 'text/plain',
        'content': request.form['Body']
    }

    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return 'OK'


def send_sms(request):
    import base64
    message = json.loads(base64.b64decode(request.json['message']['data']).decode('utf-8'))
    import config
    from twilio.rest import Client
    client = Client(config.TWILIO_ACCOUNT_SID, config.TWILIO_AUTH_TOKEN)
    client.messages.create(to=message['receiver']['value'], from_=message['sender']['value'],
                           body=message['content'])
    print(message)
    return 'OK'


def save_message(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    import base64
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    from google.cloud import firestore
    db = firestore.Client()
    message_ref = db.collection('messages').document(context.event_id)
    message['timestamp'] = context.timestamp
    message_ref.set(message)

    message['sender']['active'] = True
    person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
    persons = list(person_ref.get())
    if len(persons) == 0:
        import uuid
        message['sender']['active'] = True
        person_id = str(uuid.uuid4())
        db.collection('persons').document(person_id).set({
            'identifiers': [message['sender']]
        })
    else:
        person_id = persons[0].id

    import dialogflow_v2 as dialogflow
    df_client = dialogflow.SessionsClient()
    session = df_client.session_path(PROJECT_ID, person_id)

    text_input = dialogflow.types.TextInput(text=message['content'], language_code='en-US')
    response = df_client.detect_intent(session=session, query_input=dialogflow.types.QueryInput(text=text_input))

    if response.query_result.intent.display_name == 'connect.dexcom':
        import utils
        import uuid
        short_code = str(uuid.uuid4())
        db.collection('urls').document(short_code).set({
            'redirect': utils.create_dexcom_auth_url(person_id)
        })
        short_url = 'https://us-central1-careintent.cloudfunctions.net/u/' + short_code

        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, 'message')

        data = {
            'sender': message['receiver'],
            'receiver': message['sender'],
            'content-type': 'text/plain',
            'content': 'Visit {}'.format(short_url)
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')


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


def save_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
             event (dict): Event payload.
             context (google.cloud.functions.Context): Metadata for the event.
        """

    import base64
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    print(data)


def handle_task(request):
    print(request.json)

    from google.cloud import firestore
    db = firestore.Client()
    person_ref = db.collection('persons').document(request.json['person-id'])
    provider_ref = person_ref.collection('providers').document(request.json['provider'])
    provider = provider_ref.get().to_dict()
    import datetime
    import utils
    if 'expires' not in provider or provider['expires'] < datetime.datetime.utcnow():
        provider = utils.get_dexcom_access(provider['refresh_token'])
    last_sync = provider['last_sync'] if 'last_sync' in provider else None
    data = utils.get_dexcom_egvs(provider['access_token'], last_sync)
    if data:
        provider['last_sync'] = datetime.datetime.utcnow().isoformat()
    provider_ref.set(provider)

    if 'repeat-secs' in request.json:
        import utils
        utils.create_dexcom_polling(request.json, request.json['repeat-secs'])

    return 'OK'


def short_url(request):
    from google.cloud import firestore
    db = firestore.Client()
    url = db.collection('urls').document(request.path[1:]).get()
    import flask
    response = flask.make_response()
    if not url or not url.get('redirect'):
        response.status_code = 404
        return response

    return flask.redirect(url.get('redirect'), 302)


def handle_auth(request):
    import cipher
    state = cipher.parse_auth_token(request.args.get('state'))

    data = {'client_id': config.DEXCOM_ID,
            'client_secret': config.DEXCOM_SECRET,
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    import requests
    response = requests.post('https://sandbox-api.dexcom.com/v2/oauth2/token', data=data)
    print(response.content)

    from google.cloud import firestore
    db = firestore.Client()
    person_ref = db.collection('persons').document(state['person-id'])
    provider_ref = person_ref.collection('providers').document(state['provider'])
    provider = response.json()
    import datetime
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    provider_ref.set(provider)

    import utils
    utils.create_dexcom_polling(state, 5 * 60)

    import flask
    return flask.redirect('https://www.careintent.com', 302)
