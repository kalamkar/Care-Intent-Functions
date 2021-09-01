import base64
import config
import datetime
import json
import logging
import pytz
import uuid

import dialogflow_v2beta1 as dialogflow
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.protobuf.json_format import MessageToDict

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(request):
    tokens = request.path.split('/')
    if len(tokens) == 2 and tokens[1] == 'text':
        return process_text(request.form['From'], request.form['To'], request.form['Body'])
    elif len(tokens) == 2 and tokens[1] == 'voice':
        logging.info(request.data)
    elif len(tokens) == 3 and tokens[2] == 'status':
        logging.info(request.data)
    return '', 204


def process_text(sender, receiver, content):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

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
    now = datetime.datetime.utcnow().astimezone(pytz.utc)
    if 'start' not in person['session'] or (now - person['session']['start']).total_seconds() > config.SESSION_SECONDS:
        person['session'] = {'start': now}

    knowledge_base_path = dialogflow.KnowledgeBasesClient.knowledge_base_path(config.PROJECT_ID,
                                                                              config.SYSTEM_KNOWLEDGE_ID)
    query_params = dialogflow.types.QueryParameters(knowledge_base_names=[knowledge_base_path])
    if 'context' in person['session']:
        query_params.contexts = \
            [build_df_context(person_id, name, value) for name, value in person['session']['context'].items()]
    df_client = dialogflow.SessionsClient()
    text_input = dialogflow.types.TextInput(text=content, language_code='en-US')
    df = df_client.detect_intent(session=df_client.session_path(config.PROJECT_ID, person_id),
                                 query_input=dialogflow.types.QueryInput(text=text_input),
                                 query_params=query_params)
    logging.info(df)

    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': {'type': 'phone', 'value': sender},
        'receiver': {'type': 'phone', 'value': receiver},
        'status': 'received',
        'tags': ['source:twilio', df.query_result.intent.display_name],
        'content_type': 'text/plain',
        'content': content,
        'nlp': {
            'intent': df.query_result.intent.display_name,
            'action': df.query_result.action,
            'sentiment_score': df.query_result.sentiment_analysis_result.query_text_sentiment.score,
            'reply': df.query_result.fulfillment_text,
            'confidence': int(df.query_result.intent_detection_confidence * 100),
            'params': MessageToDict(df.query_result.parameters)
        }
    }
    if df.query_result.action:
        data['tags'].append(df.query_result.action)
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    context = get_context_dict(df.query_result.output_contexts)
    if context:
        person['session']['context'] = context
    person['session']['last_message_time'] = now
    logging.info(person['session'])
    db.collection('persons').document(person_id).update({'session': person['session']})  # Update only session part
    return '', 204


def build_df_context(session_id, name, data):
    df_context = dialogflow.types.Context(name='projects/{project}/agent/sessions/{session}/contexts/{name}'.format(
        project=config.PROJECT_ID, session=session_id, name=name))
    if 'lifespanCount' in data:
        df_context.lifespan_count = data['lifespanCount']
    if 'parameters' in data:
        df_context.parameters.update(data['parameters'])
    return df_context


def get_context_dict(contexts):
    context = {}
    for ctx in contexts:
        data = MessageToDict(ctx)
        name = data['name'].split('/')[-1]
        if name.startswith('__'):
            continue
        context[name] = data
        del data['name']
    return context
