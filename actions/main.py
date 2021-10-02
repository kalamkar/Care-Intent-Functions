import base64

import common
import config
import datetime
import generic
import json
import logging
import message
import providers
import pytz
import re
import random
import ticket
import traceback

from google.cloud import bigquery
from google.cloud import firestore

from context import Context

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())

ACTIONS = {
    'CreateAction': generic.CreateAction,
    'DataProvider': providers.DataProvider,
    'RunAction': generic.RunAction,
    'Message': message.Send,
    'Broadcast': message.Broadcast,
    'ListMessages': message.List,
    'OAuth': providers.OAuth,
    'UpdateContext': generic.UpdateContext,
    'UpdateData': generic.UpdateData,
    'UpdateRelation': generic.UpdateRelation,
    'UpdateResource': generic.UpdateResource,
    'OpenTicket': ticket.Open,
    'CloseTicket': ticket.Close,
    'ListTickets': ticket.List,
    'Webhook': generic.Webhook
}

JINJA_PARAMS = ['content', 'text']


def main(event, metadata):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    channel_name = metadata.resource['name'].split('/')[-1]
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    logging.info('Got message {}'.format(message))

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
    parents = common.get_parents(context.get('sender.id'), 'member', db)
    for coach in list(filter(lambda g: g and g.exists and g.reference.path.split('/')[0] == 'persons', parents)):
        context.set('coach', coach.to_dict() | {'id': {'type': 'person', 'value': coach.id}})
    parents.extend(common.get_parents(context.get('receiver.id'), 'member', db))

    if channel_name == 'message' and message['status'] == 'internal':
        # Run a single identified scheduled action for a person (invoked by scheduled task by sending a message)
        context.set('scheduled_action_id', message['content']['id'])
        actions = [message['content']]
    else:
        groups = list(filter(lambda g: g and g.exists and g.reference.path.split('/')[0] == 'groups', parents))
        for resource_id in [context.get('sender.id'), context.get('receiver.id')]:
            if resource_id and 'type' in resource_id and resource_id['type'] == 'group':
                groups.append(db.collection('groups').document(resource_id['value']).get())
        groups.append(db.collection('groups').document(config.SYSTEM_GROUP_ID).get())
        actions = get_actions(set(groups), db)

    context.set('min_action_priority', 0)
    logging.info('Context {}'.format(context.data))
    bq = bigquery.Client()
    for action in actions:
        try:
            process_action(action, context, bq)
        except:
            traceback.print_exc()


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
            logging.info('Skipping {id} recently run at {runtime}'.format(id=action['id'], runtime=latest_run_time))
            return

    if action['type'] not in ACTIONS or ('condition' in action and not context.evaluate(action['condition'])):
        return

    if action['priority'] < context.get('min_action_priority'):
        return

    logging.info('Triggering {}'.format(action['id']))

    params = get_context_params(action['params'], context)
    content_id, content = None, None
    if 'content' in params:
        selection = action['content_select'] if 'content_select' in action else 'random'
        content, content_id = get_content(params['content'], selection, latest_content_id)
        if not content:
            logging.warning('Skipping matched action {} because of missing content'.format(action['id']))
            return
        params['content'] = context.render(content)
    for param_name in filter(lambda p: p != 'content', JINJA_PARAMS):
        if param_name in params:
            params[param_name] = context.render(params[param_name])

    actrun = ACTIONS[action['type']]()
    actrun.process(**params)
    logging.info(actrun.context_update)
    context.update(actrun.context_update)
    if 'min_action_priority' in action:
        context.set('min_action_priority', action['min_action_priority'])
    if actrun.action_update:
        db = firestore.Client()
        parent_id = action['parent']['id']
        db.collection(common.COLLECTIONS[parent_id['type']]).document(action['id']).update(actrun.action_update)
    log = {'time': datetime.datetime.utcnow().isoformat(), 'type': 'action.run',
           'resources': [resource_id, {'type': 'action', 'value': action['id']}]}
    if content_id:
        log['resources'].append({'type': 'content', 'value': content_id})
    errors = bq.insert_rows_json('%s.live.log' % config.PROJECT_ID, [log])
    if errors:
        logging.warning(errors)
    return


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
            logging.warning('Invalid content id ' + latest_content_id)
    return content[i]['message'], content[i]['id'] if 'id' in content[i] else None


def get_latest_run_time(action_id, resource_id, bq):
    if not resource_id or 'type' not in resource_id or 'value' not in resource_id:
        return None, None
    q = '''SELECT time, content FROM(
        SELECT time,
            (SELECT value FROM UNNEST(resources) 
                WHERE type = "action") AS action,
            (SELECT value FROM UNNEST(resources) 
                WHERE type = "{resource_type}") AS resource,
            (SELECT value FROM UNNEST(resources) 
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


def get_actions(groups, db):
    actions = []
    ids = set()
    for group_doc in groups:
        group = group_doc.to_dict()
        if 'policies' not in group:
            continue
        logging.info('Applying {} policies {}'.format(group['title'], group['policies']))
        for policy_id in group['policies']:
            policy = db.collection('policies').document(policy_id).get()
            if not policy.exists:
                continue
            for action_id, action in policy.to_dict().items():
                if action_id not in ids:
                    actions.append(action | {'parent': group | {'id': common.get_id(group_doc)}})
                    ids.add(action_id)
    actions = sorted(actions, key=lambda action: action['priority'], reverse=True)
    return actions


def get_resource(resource, db):
    if not resource or type(resource) != dict or 'value' not in resource or 'type' not in resource:
        return None
    elif resource['type'] == 'phone':
        person_id = resource
        persons = list(db.collection('persons').where('identifiers', 'array_contains', person_id).get())
        if len(persons) > 0:
            return persons[0].to_dict() | {'id': {'type': 'person', 'value': persons[0].id}}
        else:
            groups = list(db.collection('groups').where('identifiers', 'array_contains', resource).get())
            return (groups[0].to_dict() | {'id': {'type': 'group', 'value': groups[0].id}}) if groups else None
    elif resource['type'] in ['person', 'group']:
        doc = db.collection(common.COLLECTIONS[resource['type']]).document(resource['value']).get()
        return doc.to_dict() | {'id': resource}


def add_shorthands(context):
    sender = context.get('sender')
    receiver = context.get('receiver')
    if receiver and type(receiver) == dict and 'id' in receiver and receiver['id']['type'] == 'person':
        context.set('person', receiver)
    elif sender and type(sender) == dict and 'id' in sender and sender['id']['type'] == 'person':
        context.set('person', sender)
    status = context.get('message.status')
    tags = context.get('message.tags') or []
    if status == 'received' and 'proxy' not in tags:
        context.set('from_member', True)
    elif status == 'received' and 'proxy' in tags:
        context.set('from_coach', True)
    elif status == 'sent' and 'proxy' in tags:
        context.set('to_coach', True)
    elif status == 'sent' and 'proxy' not in tags:
        context.set('to_member', True)


def get_context_params(action_params, context):
    params = {}
    for name, value in action_params.items():
        needs_json_load = False
        variables = re.findall(r'\$[a-z0-9-_.]+', value) if (name not in JINJA_PARAMS) and (type(value) == str) else []
        for var in variables:
            context_value = context.get(var[1:])
            if value == var:
                value = context_value
            elif type(context_value) == str:
                value = value.replace(var, context_value)
            else:
                needs_json_load = True
                try:
                    value = value.replace(var, json.dumps(context_value))
                except Exception as ex:
                    logging.warning(ex)
        try:
            params[name] = json.loads(value) if needs_json_load else value
        except Exception as ex:
            logging.warning(ex)
            logging.warning(value)
            params[name] = value
    return params
