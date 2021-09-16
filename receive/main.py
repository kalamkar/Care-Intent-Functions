import common
import config
import datetime
import json
import logging
import pytz

import dialogflow_v2beta1 as dialogflow
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.protobuf.json_format import MessageToDict

from twilio.twiml.voice_response import Dial, VoiceResponse, Say

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(request):
    tokens = request.path.split('/')
    logging.info(request.form)

    sender, receiver, content = request.form['From'], request.form['To'], request.form['Body']

    db = firestore.Client()
    contact = {'type': 'phone', 'value': sender}
    person_docs = list(db.collection('persons').where('identifiers', 'array_contains', contact).get())
    if len(person_docs) == 0:
        # Create new person since it doesn't exist
        person_id = common.generate_id()
        person = {'identifiers': [contact]}
        db.collection('persons').document(person_id).set(person)
        person['id'] = {'type': 'person', 'value': person_id}
    else:
        if len(person_docs) > 1:
            logging.warning('More than 1 person for ' + sender)
        person = person_docs[0].to_dict()
        person['id'] = {'type': 'person', 'value': person_docs[0].id}

    tags = ['source:twilio']
    sender_id = {'type': 'phone', 'value': sender}
    receiver_id = {'type': 'phone', 'value': receiver}
    if receiver in config.PROXY_PHONE_NUMBERS:
        tags.append('proxy')
        receiver_id = common.get_child_id(person['id'], receiver_id, db)
        if not receiver_id:
            logging.error(f'Missing child id for {sender}{receiver}'.format(sender=sender, receiver=receiver))

    if len(tokens) == 2 and tokens[1] == 'text':
        return process_text(sender_id, receiver_id, content, tags, person, db)
    elif len(tokens) == 2 and tokens[1] == 'voice' and request.form['Direction'] == 'inbound' and 'proxy' in tags:
        # ('CallStatus', 'ringing'), ('Direction', 'inbound')
        receiver_doc = db.collection(common.COLLECTIONS[receiver_id['type']]).document(receiver_id['value']).get()
        receiver_phone = common.filter_identifier(receiver_doc, 'phone')
        if receiver_phone:
            return process_voice_proxy(receiver_phone['value'])
    return '', 204


def process_voice_proxy(receiver):
    response = VoiceResponse()
    response.say('Connecting')
    response.dial(receiver)
    return response


def process_text(sender_id, receiver_id, content, tags, person, db):
    person_id = person['id']['value']
    now = datetime.datetime.utcnow().astimezone(pytz.utc)
    start_new_session = \
        'session' not in person \
        or 'start' not in person['session'] \
        or ((now - person['session']['start']).total_seconds() > config.SESSION_SECONDS
            and ('last_message_time' not in person['session']
                 or (now - person['session']['last_message_time']).total_seconds() > config.GAP_SECONDS))
    if start_new_session:
        person['session'] = {'start': now, 'id': common.generate_id()}

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
    sentiment_score = df.query_result.sentiment_analysis_result.query_text_sentiment.score

    data = {
        'time': datetime.datetime.utcnow().isoformat(),
        'sender': sender_id,
        'receiver': receiver_id,
        'status': 'received',
        'tags': tags + [df.query_result.intent.display_name, 'session:' + person['session']['id'],
                        'sentiment:' + str(sentiment_score)],
        'content_type': 'text/plain',
        'content': content,
        'nlp': {
            'intent': df.query_result.intent.display_name,
            'action': df.query_result.action,
            'sentiment_score': sentiment_score,
            'reply': df.query_result.fulfillment_text,
            'confidence': int(df.query_result.intent_detection_confidence * 100),
            'params': MessageToDict(df.query_result.parameters)
        }
    }

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    if df.query_result.action:
        data['tags'].append(df.query_result.action)
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    context = get_context_dict(df.query_result.output_contexts)
    if context:
        person['session']['context'] = context
    person['session']['last_message_time'] = now
    # Update only session part
    db.collection('persons').document(person_id).update({'session': person['session']})
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
