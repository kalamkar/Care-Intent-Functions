import base64
import config
import datetime
import json
import logging
import uuid

import dialogflow_v2 as dialogflow
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.protobuf.json_format import MessageToDict


class IdType(object):
    phone = 'phone'


def twilio(request):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    sender = request.form['From']
    receiver = request.form['To']
    content = request.form['Body']

    db = firestore.Client()
    contact = {'type': 'phone', 'value': sender, 'active': True}
    person_docs = list(db.collection('persons').where('identifiers', 'array_contains', contact).get())
    if len(person_docs) == 0:
        # Create new person since it doesn't exist
        person_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
        person = {'identifiers': [contact]}
        db.collection('persons').document(person_id).set(person)
    else:
        if len(person_docs) > 1:
            logging.warning('More than 1 person for ' + sender)
        person_id = person_docs[0].id
        person = person_docs[0].to_dict()

    if 'session' not in person:
        person['session'] = {}
    if 'start' not in person['session'] or \
            (datetime.datetime.utcnow() - person['session']['start']).total_seconds() > config.SESSION_SECONDS:
        person['session']['start'] = datetime.datetime.utcnow()
        if 'contexts' in person['session']:
            del person['session']['contexts']

    text_input = dialogflow.types.TextInput(text=content, language_code='en-US')
    df_contexts = person['session']['contexts'] if 'contexts' in person['session'] else None
    query_params = dialogflow.types.QueryParameters(contexts=df_contexts) if df_contexts else None
    df_client = dialogflow.SessionsClient()
    response = df_client.detect_intent(session=df_client.session_path(config.PROJECT_ID, person_id),
                                       query_input=dialogflow.types.QueryInput(text=text_input),
                                       query_params=query_params)

    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': IdType.phone, 'value': sender},
        'receiver': {'type': IdType.phone, 'value': receiver},
        'status': 'received',
        'tags': ['source:twilio', response.query_result.intent.display_name],
        'content_type': 'text/plain',
        'content': content,
        'nlp': {
            'intent': response.query_result.intent.display_name,
            'action': response.query_result.action,
            'reply': response.query_result.fulfillment_text,
            'confidence': int(response.query_result.intent_detection_confidence * 100),
            'params': MessageToDict(response.query_result.parameters)
        }
    }
    if response.query_result.action:
        data['tags'].append(response.query_result.action)
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    person['session']['contexts'] = response.query_result.output_contexts
    logging.info(person['session'])
    db.collection('persons').document(person_id).update({'session': person['session']})  # Update only session part
    return 'OK'
