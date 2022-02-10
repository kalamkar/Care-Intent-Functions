import common
import config
import datetime
import json
import logging
import pytz
import twilio.rest

from google.cloud import dialogflow_v2beta1 as dialogflow
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.protobuf.json_format import MessageToDict
from twilio.twiml.voice_response import VoiceResponse, Connect, Parameter, Hangup


TWILIO_ACCOUNT_SID = 'ACd3f03d1554da132e550d541480419d42'
TWILIO_AUTH_TOKEN = 'c05ceb45e0fc570aa45643e3ddbb0308'

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(request):
    tokens = request.path.split('/')
    logging.info(request.form)

    sender, receiver = request.form.get('From'), request.form.get('To')
    call = None
    if not sender and request.form.get('CallSid'):
        client = twilio.rest.Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        call = client.calls.get(request.form.get('CallSid')).fetch()
        sender = call.from_

    db = firestore.Client()
    contact = {'type': 'phone', 'value': sender}
    person_docs = list(db.collection('persons').where('identifiers', 'array_contains', contact).get())
    if len(person_docs) == 0:
        if receiver in config.PROXY_PHONE_NUMBERS:
            # This is spam, new person shouldn't be reaching out to proxy number themselves
            logging.warning('Received spam from %s on %s' % (sender, receiver))
            return '<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>'
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

    tags = ['source:twilio', 'zip:' + request.form.get('FromZip', default='0')]
    sender_id = {'type': 'phone', 'value': sender}
    receiver_id = {'type': 'phone', 'value': receiver}
    if receiver in config.PROXY_PHONE_NUMBERS:
        tags.append('proxy')
        receiver_id = common.get_child_id(person['id'], receiver_id, db)
        if not receiver_id:
            logging.error(f'Missing child id for {sender} {receiver}'.format(sender=sender, receiver=receiver))
            return '<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>'

    if len(tokens) >= 2 and tokens[1] == 'text':
        return process_text(sender_id, receiver_id, request.form.get('Body'), tags, person, db)
    elif len(tokens) >= 2 and tokens[1] == 'voice' and 'proxy' in tags:
        # ('CallStatus', 'ringing' or 'in-progress'), ('Direction', 'inbound'), ('DialCallStatus', 'completed')
        receiver_doc = db.collection(common.COLLECTIONS[receiver_id['type']]).document(receiver_id['value']).get()
        receiver_phone_id = common.filter_identifier(receiver_doc, 'phone')
        if tokens[-1] == 'status' and receiver_phone_id and request.form.get('DialCallStatus') == 'completed':
            params = {'coach_call_voice': 'completed', 'phone': sender}
            publish_data(receiver_phone_id, params, duration=int(request.form.get('DialCallDuration')))
        elif request.form.get('Direction') == 'inbound' and receiver_phone_id:
            return f'<?xml version="1.0" encoding="UTF-8"?><Response><Say>Connecting</Say>'\
                   '<Dial callerId="{caller}" action="{status_url}"><Number>{receiver}</Number></Dial></Response>'\
                .format(receiver=receiver_phone_id['value'], caller=config.PHONE_NUMBER,
                        status_url='https://%s-%s.cloudfunctions.net/receive/voice/status'
                                   % (config.LOCATION_ID, config.PROJECT_ID))
    elif len(tokens) >= 2 and tokens[1] == 'voice' and 'proxy' not in tags:
        # ('CallStatus', 'ringing' or 'in-progress'), ('Direction', 'inbound'), ('DialCallStatus', 'completed')
        coach_docs = list(filter(lambda g: g and g.exists and g.reference.path.split('/')[0] == 'persons',
                                 common.get_parents(person['id'], 'member', db)))
        if not coach_docs:
            logging.warning('Coach not assigned to {}'.format(sender))
            return play_audio_recording(person)
        receiver_phone_id = common.filter_identifier(coach_docs[0], 'phone')
        caller_phone_id = common.get_proxy_id({'type': 'person', 'value': coach_docs[0].id}, person['id'], db)
        if tokens[-1] == 'status' and caller_phone_id and receiver_phone_id \
                and request.form.get('DialCallStatus') == 'completed':
            params = {'member_call_voice': 'completed', 'phone': sender}
            publish_data(receiver_phone_id, params, duration=int(request.form.get('DialCallDuration')))
        elif request.form.get('Direction') == 'inbound' and caller_phone_id and receiver_phone_id:
            return f'<?xml version="1.0" encoding="UTF-8"?><Response><Say>Connecting</Say>'\
                   '<Dial callerId="{caller}" action="{status_url}"><Number>{receiver}</Number></Dial></Response>'\
                .format(receiver=receiver_phone_id['value'], caller=caller_phone_id['value'],
                        status_url='https://%s-%s.cloudfunctions.net/receive/voice/status'
                                   % (config.LOCATION_ID, config.PROJECT_ID))
    elif len(tokens) >= 2 and tokens[1] == 'status':
        if request.form.get('StreamEvent') == 'stream-stopped':
            offset = person['audio']['offset'] if 'audio' in person and 'offset' in person['audio'] else 0
            offset += int(call.duration) * 8000
            db.collection('persons').document(person['id']['value']).update({'audio.offset': offset})
            logging.info('Updated offset to %d for %s' % (offset, person['id']['value']))
            return '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
        elif request.form.get('StreamEvent') == 'stream-started':
            return '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'

    return '<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>'


def play_audio_recording(person):
    response = VoiceResponse()
    connect = Connect()
    stream = connect.stream(url='wss://websockets-dot-careintent.uc.r.appspot.com/twilio',
                            status_callback='https://us-central1-careintent.cloudfunctions.net/receive/status')
    stream.append(Parameter(name='object', value='audio/open-source-insulin-part-2-full-show.wav'))
    offset = person['audio']['offset'] if 'audio' in person and 'offset' in person['audio'] else 0
    stream.append(Parameter(name='offset', value=offset))
    logging.info('Starting from offset %d for %s' % (offset, person['id']['value']))
    response.append(connect)
    return str(response)


def process_text(sender_id, receiver_id, content, tags, person, db):
    if not content:
        logging.warning('Empty message body from ' + sender_id['value'])
        return '', 204
    person_id = person['id']['value']
    now = datetime.datetime.utcnow().astimezone(pytz.utc)
    if 'session' not in person or 'id' not in person['session']:
        person['session'] = {'start': now, 'id': common.generate_id()}
    elif not is_valid_session(person) and 'lead' in person['session']:
        del person['session']['lead']

    df_client = dialogflow.SessionsClient()
    text_input = dialogflow.types.TextInput({'text': content[:255], 'language_code': 'en-US'})
    df = df_client.detect_intent({'session': df_client.session_path(config.PROJECT_ID, person_id),
                                  'query_input': dialogflow.types.QueryInput({'text': text_input})})
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
            'confidence': int(df.query_result.intent_detection_confidence * 100)
        }
    }

    try:
        data['nlp']['params'] = MessageToDict(df.query_result.parameters)
    except:
        logging.error('Could not convert parameters')

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

    if df.query_result.action:
        data['tags'].append(df.query_result.action)
    publisher.publish(topic_path, json.dumps(data).encode('utf-8'))

    person['session']['last_message_time'] = now
    # Update only session part
    db.collection('persons').document(person_id).update({'session': person['session']})
    return '', 204


def publish_data(person_id, params, tags=(), duration=None):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

    row = {
        'time': datetime.datetime.utcnow().isoformat(),
        'source': person_id,
        'tags': tags,
        'data': []
    }
    if duration is not None:
        row['duration'] = duration
    for name, value in params.items():
        if type(value) in [int, float]:
            row['data'].append({'name': name, 'number': value})
        elif type(value) == str:
            row['data'].append({'name': name, 'value': value})
    publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


def is_valid_session(person):
    now = datetime.datetime.utcnow().astimezone(pytz.utc)
    return 'session' in person and 'last_message_time' in person['session']\
           and (now - person['session']['last_message_time']).total_seconds() < config.GAP_SECONDS

