import base64
import datetime

import pytz

import config
import json
import generic
import re

import dialogflow_v2 as dialogflow
from google.cloud import bigquery
from google.cloud import firestore


ACTIONS = {'Message': generic.Message,
           'OAuthMessage': generic.OAuthMessage,
           'SimplePatternCheck': generic.SimplePatternCheck,
           'Update': generic.Update}


def process(event, metadata):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    db = firestore.Client()
    context = Context()

    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    print(metadata)

    person = None
    if metadata.resource['name'].endswith('/message'):
        context.set('message', message)
        message['sender']['active'] = True
        person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
        persons = list(person_ref.get())
        if len(persons) > 0:
            person = persons[0]
    elif metadata.resource['name'].endswith('/data'):
        context.set('data', message)
        person = db.collection('persons').document(message['source']['id']).get()

    if not person:
        return 500, 'Not ready'

    context.set('sender', person.to_dict())
    context.set('sender.id', person.id)

    if 'status' in message and message['status'] == 'received':
        df_client = dialogflow.SessionsClient()
        session = df_client.session_path(config.PROJECT_ID, person.id)
        text_input = dialogflow.types.TextInput(text=message['content'], language_code='en-US')
        query_params = dialogflow.types.QueryParameters()
        df_data = person.get('dialogflow')
        if df_data and 'context' in df_data:
            query_params.context = [get_df_context(df_data['context'], person.id)]
            if 'lifespan' in df_data['context']:
                df_data['context']['lifespan'] -= 1
                if df_data['context']['lifespan'] < 1:
                    person_dict = person.to_dict()
                    del person_dict['dialogflow']['context']
                    person_ref = db.collection('persons').document(person.id)
                    person_ref.update(person_dict)
        query = dialogflow.types.QueryInput(text=text_input)
        response = df_client.detect_intent(session=session, query_input=query, query_params=query_params)
        context.set('dialogflow', {
            'intent': response.query_result.intent.display_name,
            'action': response.query_result.action,
            'fulfillment-text': response.query_result.fulfillment_text,
            'confidence': int(response.query_result.intent_detection_confidence * 100),
            'params': response.query_result.parameters
        })

    print(context.data)

    client = bigquery.Client()
    actions = list(db.collection('actions').get())
    actions.sort(key=lambda a: a.get('priority'), reverse=True)
    for action_doc in actions:
        action = action_doc.to_dict()
        score = 0
        for rule in action['rules']:
            context_value = context.get(rule['name'])
            expected_value = rule['value']
            if rule['compare'] == 'str' and context_value == expected_value:
                score = rule['weight']
            elif rule['compare'] == 'regex' and context_value:
                matches = re.findall(expected_value, context_value)
                if matches:
                    context.set(rule['name'] + '_match', matches)
                    score = rule['weight']
            elif rule['compare'] == 'number' and context_value == float(expected_value):
                score = rule['weight']

        if score and action['type'] in ACTIONS:
            if 'repeat-secs' in action:
                latest_run_time = get_latest_run_time(action_doc.id, person.id)
                threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=action['repeat-secs'])
                if latest_run_time and latest_run_time > threshold.astimezone(pytz.UTC):
                    print('Skipping {action} recently run at {runtime}'.format(action=action['type'],
                                                                               runtime=latest_run_time))
                    continue

            action['score'] = score
            params = {}
            for name, value in action['params'].items():
                params[name] = context.get(value[1:]) if type(value) == str and value.startswith('$') else value

            action_object = ACTIONS[action['type']](**params)
            action_object.process()
            print(action_object.output)
            context.update(action_object.output)
            log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
                   'resources': [{'type': 'person', 'id': person.id},
                                 {'type': 'action', 'id': action_doc.id}]}
            errors = client.insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
            if errors:
                print(errors)


def get_latest_run_time(action_id, person_id):
    client = bigquery.Client()
    query = '''SELECT time FROM(
        SELECT time,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "action") AS action,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "person") AS person
        FROM `careintent.live.log`
        WHERE type = "action.run"
    )
    WHERE action = "{action_id}" AND person = "{person_id}"
    ORDER BY time DESC LIMIT 1'''.format(person_id=person_id, action_id=action_id)
    latest_run_time = None
    for row in client.query(query):
        latest_run_time = row['time']
    return latest_run_time


class Context(object):
    def __init__(self):
        self.data = {}

    def set(self, name, value):
        tokens = name.split('.') if name else []
        if len(tokens) == 1:
            self.data[name] = value
        elif len(tokens) == 2:
            self.data[tokens[0]][tokens[1]] = value
        elif len(tokens) == 3:
            self.data[tokens[0]][tokens[1]][tokens[2]] = value
        elif len(tokens) == 4:
            self.data[tokens[0]][tokens[1]][tokens[2]][tokens[3]] = value

    def get(self, name):
        tokens = name.split('.') if name else []
        try:
            if len(tokens) == 1:
                return self.data[tokens[0]]
            elif len(tokens) == 2:
                return self.data[tokens[0]][tokens[1]]
            elif len(tokens) == 3:
                return self.data[tokens[0]][tokens[1]][tokens[2]]
            elif len(tokens) == 4:
                return self.data[tokens[0]][tokens[1]][tokens[2]][tokens[3]]
        except KeyError:
            return None
        return None

    def update(self, patch):
        self.data.update(patch)


def get_df_context(context, session_id):
    context_name = "projects/" + config.PROJECT_ID + "/agent/sessions/" + session_id + "/contexts/" +\
                   context['name'].lower()
    parameters = dialogflow.types.struct_pb2.Struct()
    if 'params' in context:
        parameters.update(context['params'])
    return dialogflow.types.context_pb2.Context(
        name=context_name,
        lifespan_count=context['lifespan'] if 'lifespan' in context else 1,
        parameters=parameters
    )
