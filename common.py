import base64
import config
import datetime
import json
import logging
import pytz
import uuid

COLLECTIONS = {'person': 'persons', 'group': 'groups', 'message': 'messages', 'member': 'members', 'admin': 'admins',
               'schedule': 'schedules'}

DURATIONS = {
    'm': 60,
    'h': 60 * 60,
    'd': 24 * 60 * 60,
    'w': 7 * 24 * 60 * 60,
}


def get_duration_secs(duration):
    if duration[-1] not in DURATIONS:
        return 0
    return DURATIONS[duration[-1]] * int(duration[:-1])


def get_parents(child_id, child_type, db):
    if not child_id or child_type not in COLLECTIONS:
        return []
    relation_query = db.collection_group(COLLECTIONS[child_type]).where('id', '==', child_id)
    return [relative.reference.parent.parent.get() for relative in relation_query.stream()]


def get_children_ids(parent_id, child_type, db):
    relation_query = db.collection(COLLECTIONS[parent_id['type']]).document(parent_id['value']) \
        .collection(COLLECTIONS[child_type])
    return [doc.get('id') for doc in relation_query.stream()]


def get_id(doc):
    if not doc:
        return None
    return {'type': doc.reference.path.split('/')[-2][:-1], 'value': doc.id}


def schedule_task(payload, client, timestamp=None, queue_name='actions'):
    queue = client.queue_path(config.PROJECT_ID, config.LOCATION_ID, queue_name)
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': 1,  # tasks_v2.HttpMethod.POST,
            'url': 'https://%s-%s.cloudfunctions.net/process-task' % (config.LOCATION_ID, config.PROJECT_ID),
            'oidc_token': {'service_account_email': '%s@appspot.gserviceaccount.com' % config.PROJECT_ID},
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        }
    }
    if timestamp:
        task['schedule_time'] = timestamp
    response = client.create_task(request={'parent': queue, 'task': task})
    logging.info("Created task {}".format(response.name))
    return response.name


def get_identifier(identifier, id_type, db, default=None, resource_types=('person', 'group')):
    if not identifier or type(identifier) != dict or 'value' not in identifier or 'type' not in identifier:
        return default
    elif identifier['type'] == id_type:
        return identifier
    elif identifier['type'] in resource_types:
        return filter_identifier(db.collection(COLLECTIONS[identifier['type']]).document(identifier['value']).get(),
                                 id_type)
    return default


def filter_identifier(resource_doc, id_type, default=None):
    if not resource_doc:
        return default
    ids = resource_doc.get('identifiers')
    if not ids:
        return default
    phones = list(filter(lambda i: i['type'] == id_type, ids))
    return phones[0] if phones else default


def generate_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')


def get_proxy_id(parent_id, child_id, db, assign=False):
    proxy_numbers = config.PROXY_PHONE_NUMBERS.copy() if assign else []
    for child in db.collection(COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
            .collection('members').stream():
        proxy = child.get('proxy')
        if child.get('id') == child_id:
            return proxy
        if proxy['value'] in proxy_numbers:
            proxy_numbers.remove(proxy['value'])
    return {'type': 'phone', 'value': proxy_numbers[0]} if proxy_numbers else None


def get_child_id(parent_id, proxy_id, db):
    for child in db.collection(COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
            .collection('members').stream():
        if child.get('proxy') == proxy_id:
            return child.get('id')
    return None


def add_child(child_id, parent_id, relation_type, db):
    data = {'id': child_id}
    if parent_id['type'] == 'person':
        data['proxy'] = get_proxy_id(parent_id, child_id, db, assign=True)
        if not data['proxy']:
            logging.error('Proxy number not available, UpdateRelation failed.')
            return None
    db.collection(COLLECTIONS[parent_id['type']]).document(parent_id['value']) \
        .collection(COLLECTIONS[relation_type]).document(child_id['type'] + ':' + child_id['value']).set(data)
    return data
