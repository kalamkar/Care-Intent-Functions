import base64
import datetime

import config
import json
import generic
import re

import dialogflow_v2 as dialogflow
from google.cloud import bigquery
from google.cloud import firestore


ACTIONS = {'Message': generic.Message,
           'OAuthMessage': generic.OAuthMessage,
           'SimplePatternCheck': generic.SimplePatternCheck}


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

    person_id = None
    if metadata.resource['name'].endswith('/message'):
        message['sender']['active'] = True
        person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
        persons = list(person_ref.get())
        if len(persons) == 0:
            return 500, 'Not ready'

        person_id = persons[0].id
        context.set('message', message)
        context.set('sender', persons[0].to_dict())
        context.set('sender.id', persons[0].id)
    elif metadata.resource['name'].endswith('/data'):
        context.set('data', message)
        person_id = message['source']['id']

    if 'status' in message and message['status'] == 'received':
        df_client = dialogflow.SessionsClient()
        session = df_client.session_path(config.PROJECT_ID, persons[0].id)
        text_input = dialogflow.types.TextInput(text=message['content'], language_code='en-US')
        response = df_client.detect_intent(session=session, query_input=dialogflow.types.QueryInput(text=text_input))
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
    print([action.get('type') for action in actions])
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
            action['score'] = score
            params = {}
            for name, value in action['params'].items():
                params[name] = context.get(value[1:]) if type(value) == str and value.startswith('$') else value

            print(action['type'])
            action_object = ACTIONS[action['type']](**params)
            action_object.process()
            print(action_object.output)
            context.update(action_object.output)
            log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
                   'resources': [{'type': 'person', 'id': person_id},
                                 {'type': 'action', 'id': action_doc.id}]}
            errors = client.insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
            if errors:
                print(errors)


# def get_latest_run_time(action_id, source_id):
#     client = bigquery.Client()
#     query = 'SELECT time FROM careintent.live.log, UNNEST(resources) ' \
#             'WHERE type = "action" AND id = "{action_id}" ' \
#             'AND time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {seconds} second) ' \
#             'ORDER BY time'. \
#         format(source=self.person_id, name=self.name, seconds=self.seconds)
#     data = []
#     for row in client.query(query):
#         data.append((row['time'], row['number']))


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
