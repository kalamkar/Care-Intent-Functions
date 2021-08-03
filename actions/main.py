import base64
import config
import datetime
import generic
import json
import pytz
import query
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
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    print(metadata, message)

    db = firestore.Client()
    context = Context()
    context.set(channel_name, message)
    person = None
    if channel_name == 'message':
        person_id = {'active': True}
        person_id.update(message['sender'] if 'sender' in message else message['receiver'])
        if 'type' in person_id and person_id['type'] == 'person':
            person = db.collection('persons').document(person_id['value']).get()
        else:
            person_ref = db.collection('persons').where('identifiers', 'array_contains', person_id)
            persons = list(person_ref.get())
            if len(persons) > 0:
                person = persons[0]
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data.' + data['name'], data['number'] if 'number' in data else data['value'])
        person = db.collection('persons').document(message['source']['id']).get()

    if not person:
        return 500, 'Not ready'

    context.set('sender', person.to_dict())
    context.set('sender.id', person.id)

    if 'dialogflow' in message :
        context.set('dialogflow', message['dialogflow'])

    print(context.data)

    bq = bigquery.Client()
    if message['content_type'] == 'application/json' and 'action_id' in message['content']:
        actions = [db.collection('groups').document(message['group_id'])
                       .collection('actions').document(message['action_id']).get().to_dict()]
    else:
        actions = get_actions(person.id)
    for action in actions:
        if 'hold_secs' in action:
            latest_run_time = get_latest_run_time(action['id'], person.id, bq)
            threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=action['hold_secs'])
            if latest_run_time and latest_run_time > threshold.astimezone(pytz.UTC):
                print('Skipping {action} recently run at {runtime}'.format(action=action['type'],
                                                                           runtime=latest_run_time))
                continue

        if action['type'] not in ACTIONS or not context.evaluate(action['condition']):
            continue

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
        errors = bq.insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
        if errors:
            print(errors)


def get_latest_run_time(action_id, person_id, bq):
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
    for row in bq.query(query):
        latest_run_time = row['time']
    return latest_run_time


def get_actions(person_id):
    db = firestore.Client()
    actions = json.load(open('data.json'))['actions']
    for group_id in query.get_relatives({'type': 'person', 'value': person_id}, ['member_of'], []):
        if not group_id or group_id['type'] != 'group':
            continue
        for action_doc in db.collection('groups').document(group_id['value']).collection('actions').stream():
            action = action_doc.to_dict()
            if 'schedule' not in action:
                action['id'] = action_doc.id
                actions.append(action)
    return sorted(actions, key=lambda a: a['priority'], reverse=True)


class Context(object):
    def __init__(self):
        self.data = {}

    def evaluate(self, expression):
        for var in set(re.findall(r'\$[a-z0-9.-_]+', expression)):
            value = self.get(var[1:])
            expression = expression.replace(var, '"{}"'.format(value) if type(value) == str else str(value))
        try:
            return eval(expression)
        except:
            return None

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
