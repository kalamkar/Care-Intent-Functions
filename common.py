import base64
import config
import json
import logging
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
    return filter(lambda g: g, [relative.reference.parent.parent.get() for relative in relation_query.stream()])


def get_children_ids(parent_id, child_type, db):
    relation_query = db.collection(COLLECTIONS[parent_id['type']]).document(parent_id['value']) \
        .collection(COLLECTIONS[child_type])
    return [doc.get('id') for doc in relation_query.stream()]


def get_id(doc):
    if not doc:
        return None
    return {'type': doc.reference.path.split('/')[-2][:-1], 'value': doc.id}


def schedule_task(payload, client, timestamp=None, queue_name='actions'):
    queue = client.queue_path(config.PROJECT_ID, 'us-central1', queue_name)
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


def get_phone_id(identifier, db, default=None, resource_types=('person', 'group')):
    if not identifier or type(identifier) != dict or 'value' not in identifier or 'type' not in identifier:
        return default
    elif identifier['type'] == 'phone':
        return identifier
    elif identifier['type'] in resource_types:
        return filter_phone_identifier(
            db.collection(COLLECTIONS[identifier['type']]).document(identifier['value']).get())
    return default


def filter_phone_identifier(resource_doc, default=None):
    if not resource_doc:
        return default
    ids = resource_doc.get('identifiers')
    if not ids:
        return default
    phones = list(filter(lambda i: i['type'] == 'phone', ids))
    return phones[0] if phones else default


def generate_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
