import base64

import common
import config
import datetime
import generic
import json
import pytz
import re
import random
import traceback

from google.cloud import bigquery
from google.cloud import firestore

from context import Context

ACTIONS = {
    'DataExtract': generic.DataExtract,
    'Message': generic.Message,
    'OAuth': generic.OAuth,
    'SimplePatternCheck': generic.SimplePatternCheck,
    'Update': generic.Update,
    'Webhook': generic.Webhook
}

JINJA_PARAMS = ['content', 'text']


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
    if channel_name == 'message':
        if 'sender' in message:
            context.set('sender', get_resource(message['sender'], db))
        if 'receiver' in message:
            context.set('receiver', get_resource(message['receiver'], db))
    elif channel_name == 'data':
        for data in message['data']:
            context.set('data', {data['name']: data['number'] if 'number' in data else data['value']})
        context.set('sender', get_resource(message['source'], db))

    add_shorthands(context)
    if channel_name == 'message' and message['content_type'] == 'application/json'\
            and 'action_id' in message['content']:
        # Run a single identified scheduled action for a person (invoked by scheduled task by sending a message)
        context.set('scheduled', True)
        action = db.collection('groups').document(message['content']['group_id'])\
            .collection('actions').document(message['content']['action_id']).get()
        group = db.collection('groups').document(message['content']['group_id']).get().to_dict()
        actions = [action.to_dict() | {'id': action.id, 'group': group}]
    else:
        actions = get_actions([context.get('sender.id'), context.get('receiver.id')])
    print('Context', context.data)
    bq = bigquery.Client()
    for action in actions:
        process_action(action, context, bq)


def process_action(action, context, bq):
    resource_id = context.get('sender.id') or context.get('receiver.id')
    context.clear('action')
    context.set('action', action)
    latest_run_time, latest_content_id = None, None
    if 'hold_secs' in action or ('content_select' in action and action['content_select'] != 'random'):
        latest_run_time, latest_content_id = get_latest_run_time(action['id'], resource_id, bq)
    if 'hold_secs' in action:
        threshold = datetime.datetime.utcnow() - datetime.timedelta(seconds=action['hold_secs'])
        if latest_run_time and latest_run_time > threshold.astimezone(pytz.UTC):
            print('Skipping {action} recently run at {runtime}'.format(action=action['type'],
                                                                       runtime=latest_run_time))
            return

    if action['type'] not in ACTIONS or ('condition' in action and not context.evaluate(action['condition'])):
        return

    params = get_context_params(action['params'], context)
    content_id, content = None, None
    if 'content' in params:
        selection = action['content_select'] if 'content_select' in action else 'random'
        content, content_id = get_content(params['content'], selection, latest_content_id)
        if not content:
            print('Skipping matched {action} action because of missing content'.format(action=action['type']))
            return
        params['content'] = context.render(content)
    for param_name in filter(lambda p: p != 'content', JINJA_PARAMS):
        if param_name in params:
            params[param_name] = context.render(params[param_name])

    try:
        print('Triggering ', action)
        actrun = ACTIONS[action['type']](**params)
        actrun.process()
        print(actrun.output)
        context.update(actrun.output)
        log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
               'resources': [{'type': resource_id['type'], 'id': resource_id['value']},
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
            i = int(latest_content_id) + 1 - 1  # Increment to next id but subtract 1 to make it 0-indexed
            if i >= len(content):  # For sequential content, stop sending messages after exhausting the list
                return None, None
        except:
            print('Invalid content id ' + latest_content_id)
    return content[i]['message'], content[i]['id'] if 'id' in content[i] else None


def get_latest_run_time(action_id, resource_id, bq):
    if not resource_id or 'type' not in resource_id or 'value' not in resource_id:
        return None, None
    q = '''SELECT time, content FROM(
        SELECT time,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "action") AS action,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "{resource_type}") AS resource,
            (SELECT id FROM UNNEST(resources) 
                WHERE type = "content") AS content
        FROM `{project}.live.log`
        WHERE type = "action.run"
    )
    WHERE action = "{action_id}" AND resource = "{resource_id}"
    ORDER BY time DESC LIMIT 1'''.format(resource_type=resource_id['type'], resource_id=resource_id['value'],
                                         action_id=action_id, project=config.PROJECT_ID)
    latest_run_time = None
    latest_content_id = None
    for row in bq.query(q):
        latest_run_time = row['time']
        latest_content_id = row['content']
    return latest_run_time, latest_content_id


def get_actions(resource_ids):
    db = firestore.Client()
    actions = json.load(open('data.json'))['actions']
    ids = set()
    for resource_id in resource_ids:
        if not resource_id:
            continue
        for group_id in common.get_relatives(resource_id, ['member_of'], []):
            if not group_id or group_id['type'] != 'group':
                continue
            for action_doc in db.collection('groups').document(group_id['value']).collection('actions').stream():
                action = action_doc.to_dict()
                if action_doc.id not in ids:
                    action['id'] = action_doc.id
                    action['group'] = db.collection('groups').document(group_id['value']).get().to_dict()
                    actions.append(action)
                    ids.add(action_doc.id)
    return sorted(actions, key=lambda a: a['priority'], reverse=True)


def get_resource(resource, db):
    if not resource or type(resource) != dict or 'value' not in resource or 'type' not in resource:
        return None
    elif resource['type'] == 'phone':
        person_id = resource | {'active': True}
        persons = list(db.collection('persons').where('identifiers', 'array_contains', person_id).get())
        if len(persons) > 0:
            return persons[0].to_dict() | {'id': {'type': 'person', 'value': persons[0].id}}
        else:
            groups = list(db.collection('groups').where('identifiers', 'array_contains', resource).get())
            return (groups[0].to_dict() | {'id': {'type': 'group', 'value': groups[0].id}}) if groups else None
    elif resource['type'] in ['person', 'group']:
        db = firestore.Client()
        doc = db.collection(resource['type'] + 's').document(resource['value']).get()
        return doc.to_dict() | {'id': resource}


def add_shorthands(context):
    sender = context.get('sender')
    receiver = context.get('receiver')
    if receiver and type(receiver) == dict and 'id' in receiver and receiver['id']['type'] == 'person':
        context.set('person', receiver)
    elif sender and type(sender) == dict and 'id' in sender and sender['id']['type'] == 'person':
        context.set('person', sender)


def get_context_params(action_params, context):
    params = {}
    for name, value in action_params.items():
        variables = re.findall(r'\$[a-z-_.]+', value) if (name not in JINJA_PARAMS) and (type(value) == str) else []
        for var in variables:
            context_value = context.get(var[1:])
            if value == var:
                value = context_value
            elif type(context_value) == str:
                value = value.replace(var, context_value)
            else:
                try:
                    value = json.loads(value.replace(var, json.dumps(context_value)))
                except Exception as ex:
                    print(ex)
        params[name] = value
    return params
