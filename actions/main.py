import base64
import config
import json
import generic
import re

import dialogflow_v2 as dialogflow
from google.cloud import firestore


ACTIONS = {'Message': generic.Message,
           'OAuthMessage': generic.OAuthMessage}


def process(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    message['sender']['active'] = True
    db = firestore.Client()
    person_ref = db.collection('persons').where('identifiers', 'array_contains', message['sender'])
    persons = list(person_ref.get())
    if len(persons) == 0:
        return 500, 'Not ready'

    context = Context()
    context.set('message', message)
    context.set('sender', persons[0].to_dict())
    context.set('sender.id', persons[0].id)

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

    # TODO: Make action execution chained.
    #  Also action creating output that is added to context for next ones, in chain, to use.
    matched = []
    for doc in db.collection('actions').stream():
        action = doc.to_dict()
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

        if score:
            action['score'] = score
            matched.append(action)

    if not matched:
        return

    for action in matched:
        params = {}
        for name, value in action['params'].items():
            params[name] = context.get(value[1:]) if value.startswith('$') else value

        if action['type'] in ACTIONS:
            ACTIONS[action['type']](**params).process()


class Context(object):
    def __init__(self):
        self.data = {}

    def set(self, name, value):
        self.data[name] = value

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
