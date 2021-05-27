import base64
import json
import re

from google.cloud import firestore
from generic import OAuthMessage


ACTIONS = {'OAuthMessage': OAuthMessage}


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

    matched = []
    for doc in db.collection('actions').stream():
        action = doc.to_dict()
        score = 0
        for rule in action['rules']:
            if rule['compare'] == 'str' and context.get(rule['name']) == rule['value']:
                score = rule['weight']
            elif rule['compare'] == 'regex':
                matches = re.findall(rule['value'], context.get(rule['name']))
                if matches:
                    context.set(rule['name'] + '_match', matches)
                    score = rule['weight']
            elif rule['compare'] == 'number' and context.get(rule['name']) == float(rule['value']):
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
