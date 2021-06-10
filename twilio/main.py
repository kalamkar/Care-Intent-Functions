import datetime
import json
import uuid

import dialogflow_v2 as dialogflow
from google.cloud import firestore
from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


class IdType(object):
    phone = 'phone'


def twilio(request):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, 'message')

    sender = request.form['From']
    receiver = request.form['To']
    content = request.form['Body']

    db = firestore.Client()
    contact = {'type': 'phone', 'value': sender, 'active': True}
    person_ref = db.collection('persons').where('identifiers', 'array_contains', contact)
    persons = list(person_ref.get())
    if len(persons) == 0:
        # Create new person since it doesn't exist
        person_id = str(uuid.uuid4())
        person = {'identifiers': [contact]}
        db.collection('persons').document(person_id).set(person)
    else:
        person_id = persons[0].id
        person = persons[0].to_dict()

    df_client = dialogflow.SessionsClient()
    session = df_client.session_path(PROJECT_ID, person_id)
    text_input = dialogflow.types.TextInput(text=content, language_code='en-US')
    query_params = None
    if 'dialogflow' in person and 'context' in person['dialogflow']:
        df_context = person['dialogflow']['context']
        query_params = dialogflow.types.QueryParameters(contexts=[get_df_context(df_context, person_id)])
        if 'lifespan' in df_context:
            df_context['lifespan'] -= 1
            if df_context['lifespan'] < 1:
                del person['dialogflow']['context']
            db.collection('persons').document(person_id).update(person)
    query = dialogflow.types.QueryInput(text=text_input)
    response = df_client.detect_intent(session=session, query_input=query, query_params=query_params)

    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': IdType.phone, 'value': sender},
        'receiver': {'type': IdType.phone, 'value': receiver},
        'status': 'received',
        'content_type': 'text/plain',
        'content': content,
        'dialogflow': {
            'intent': response.query_result.intent.display_name,
            'action': response.query_result.action,
            'fulfillment-text': response.query_result.fulfillment_text,
            'confidence': int(response.query_result.intent_detection_confidence * 100),
            'params': response.query_result.parameters
        }
    }

    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))
    return 'OK'


def get_df_context(context, session_id):
    context_name = "projects/" + PROJECT_ID + "/agent/sessions/" + session_id + "/contexts/" + context['name'].lower()
    parameters = dialogflow.types.struct_pb2.Struct()
    if 'params' in context:
        parameters.update(context['params'])
    return dialogflow.types.context_pb2.Context(
        name=context_name,
        lifespan_count=context['lifespan'] if 'lifespan' in context else 1,
        parameters=parameters
    )
