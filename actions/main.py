import base64
import sys

import config
import datetime
import generic
import jinja2
import json
import numpy as np
import pytz
import query
import re
import random
import traceback

from inspect import getmembers, isfunction

from google.cloud import bigquery
from google.cloud import firestore


ACTIONS = {
    'DataExtract': generic.DataExtract,
    'Message': generic.Message,
    'OAuth': generic.OAuth,
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
    if channel_name == 'message' and message['content_type'] == 'application/json'\
            and 'action_id' in message['content']:
        # Run a single identified scheduled action for a person (invoked by scheduled task by sending a message)
        action = db.collection('groups').document(message['content']['group_id'])\
            .collection('actions').document(message['content']['action_id']).get()
        actions = [{**(action.to_dict()), 'id': action.id}]
    else:
        actions = get_actions(person.id)
    for action in actions:
        latest_run_time, latest_content_id = None, None
        if 'hold_secs' in action or ('content_select' in action and action['content_select'] != 'random'):
            latest_run_time, latest_content_id = get_latest_run_time(action['id'], person.id, bq)
        if 'hold_secs' in action:
            threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=action['hold_secs'])
            if latest_run_time and latest_run_time > threshold.astimezone(pytz.UTC):
                print('Skipping {action} recently run at {runtime}'.format(action=action['type'],
                                                                           runtime=latest_run_time))
                continue

        if action['type'] not in ACTIONS or ('condition' in action and not context.evaluate(action['condition'])):
            continue

        params = {}
        for name, value in action['params'].items():
            variables = re.findall(r'\$[a-z-_.]+', value) if name != 'content' and type(value) == str else []
            for var in variables:
                value = context.get(var[1:]) if value == var else value.replace(var, context.get(var[1:]))
            params[name] = value

        content_id, content = None, None
        if 'content' in params:
            selection = action['content_select'] if 'content_select' in action else 'random'
            content, content_id = get_content(params['content'], selection, latest_content_id)
            if not content:
                print('Skipping matched {action} action because of missing content'.format(action=action['type']))
                continue
            params['content'] = context.render(content)

        try:
            actrun = ACTIONS[action['type']](**params)
            actrun.process()
            print(actrun.output)
            context.update(actrun.output)
            log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
                   'resources': [{'type': 'person', 'id': person.id},
                                 {'type': 'action', 'id': action['id']}]}
            if content_id:
                log['resources'].append({'type': 'content', 'id': content_id})
            errors = bq.insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
            if errors:
                print(errors)
        except:
            traceback.print_exc()


def get_content(content, select, latest_content_id):
    if type(content) == str:
        return content, None
    if type(content) != list or len(content) < 1:
        return None, None
    i = 0
    if select == 'random':
        i = random.randint(0, len(content) - 1)
    elif latest_content_id:
        try:
            i = int(latest_content_id) + 1
            if i >= len(content):  # For sequential content, stop sending messages after exhausting the list
                return None, None
        except:
            print('Invalid content id ' + latest_content_id)
    return content[i]['message'], content[i]['id'] if 'id' in content[i] else None


def get_latest_run_time(action_id, person_id, bq):
    q = '''SELECT time, content FROM(
        SELECT time,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "action") AS action,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "person") AS person,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "content") AS content
        FROM `{project}.live.log`
        WHERE type = "action.run"
    )
    WHERE action = "{action_id}" AND person = "{person_id}"
    ORDER BY time DESC LIMIT 1'''.format(person_id=person_id, action_id=action_id, project=config.PROJECT_ID)
    latest_run_time = None
    latest_content_id = None
    for row in bq.query(q):
        latest_run_time = row['time']
        latest_content_id = row['content']
    return latest_run_time, latest_content_id


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
        self.env = jinja2.Environment(loader=jinja2.BaseLoader(), trim_blocks=True, lstrip_blocks=True)
        self.env.filters['history'] = self.history
        self.env.filters['np'] = self.numpy

    def numpy(self, value, function):
        functions = {name: value for name, value in getmembers(np, isfunction)}
        if not function or function not in functions:
            return value
        return functions[function](value)

    def history(self, var, duration='1w'):
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=query.get_duration_secs(duration))
        start_time = start_time.isoformat()
        bq = bigquery.Client()
        q = 'SELECT number FROM {project}.live.tsdatav1, UNNEST(data) ' \
            'WHERE name = "{name}" AND time > TIMESTAMP("{start}") ORDER BY time'. \
            format(project=config.PROJECT_ID, name=var, start=start_time)
        print(q)
        return [row['number'] for row in bq.query(q)]

    def evaluate(self, expression):
        try:
            return self.env.from_string(expression).render(self.data) == str(True)
        except:
            return False

    def render(self, content):
        if type(content) == str:
            try:
                return self.env.from_string(content).render(self.data)
            except:
                print('Failed rendering ' + content)
        elif type(content) == list:
            return [self.render(item) for item in content]
        elif type(content) == dict:
            return {self.render(name): self.render(value) for name, value in content.items()}
        return content

    def set(self, name, value):
        if type(value) == dict:
            value = {k.replace('-', '_'): v for k,v in value.items()}
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
