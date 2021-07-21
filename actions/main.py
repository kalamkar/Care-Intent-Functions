import base64
import data
import datetime

import pytz

import config
import json
import generic
import re

from google.cloud import bigquery
from google.cloud import firestore


ACTIONS = {
    'DataExtract': generic.DataExtract,
    'Message': generic.Message,
    'OAuthMessage': generic.OAuthMessage,
    'SimplePatternCheck': generic.SimplePatternCheck,
    'Update': generic.Update
}


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

    print(metadata, message)

    person = None
    if metadata.resource['name'].endswith('/message'):
        context.set('message', message)
        person_id = {'active': True}
        person_id.update(message['sender'] if 'sender' in message else message['receiver'])
        person_ref = db.collection('persons').where('identifiers', 'array_contains', person_id)
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

    if 'dialogflow' in message :
        context.set('dialogflow', message['dialogflow'])

    print(context.data)

    client = bigquery.Client()
    for action in get_actions():
        score = 0
        for rule in action['rules']:
            context_value = context.get(rule['name'])
            expected_value = rule['value'] if 'value' in rule else None
            if rule['compare'] == 'str' and context_value == expected_value:
                score += rule['weight']
            elif rule['compare'] == 'regex' and context_value:
                matches = re.findall(expected_value, context_value)
                if matches:
                    context.set(rule['name'] + '_match', matches)
                    score += rule['weight']
            elif rule['compare'] == 'number' and context_value == float(expected_value):
                score += rule['weight']
            elif rule['compare'] == 'isnull' and context_value is None:
                score += rule['weight']
            elif rule['compare'] == 'notnull' and context_value is not None:
                score += rule['weight']

        if score >= 100 and action['type'] in ACTIONS:
            if 'repeat-secs' in action:
                latest_run_time = get_latest_run_time(action['id'], person.id)
                threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=action['repeat-secs'])
                if latest_run_time and latest_run_time > threshold.astimezone(pytz.UTC):
                    print('Skipping {action} recently run at {runtime}'.format(action=action['type'],
                                                                               runtime=latest_run_time))
                    continue

            action['score'] = score
            params = {}
            for name, value in action['params'].items():
                variables = re.findall(r'\$[a-z-_.]+', value) if type(value) == str else []
                for var in variables:
                    value = context.get(var[1:]) if value == var else value.replace(var, context.get(var[1:]))
                params[name] = value

            action_object = ACTIONS[action['type']](**params)
            action_object.process()
            print(action_object.output)
            context.update(action_object.output)
            log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
                   'resources': [{'type': 'person', 'id': person.id},
                                 {'type': 'action', 'id': action['id']}]}
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


def get_actions():
    return sorted(data.ACTIONS, key=lambda a: a.get('priority'), reverse=True)
    # actions = list(db.collection('actions').get())
    # actions.sort(key=lambda a: a.get('priority'), reverse=True)
    # return [{**(action_doc.to_dict()), {'id': action_doc.id}} for action_doc in actions]


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
