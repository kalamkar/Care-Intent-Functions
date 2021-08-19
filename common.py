import config
import json
import logging

COLLECTIONS = {'person': 'persons', 'group': 'groups', 'message': 'messages', 'schedule': 'schedules',
               'member': 'members', 'admin': 'admins'}

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
            'url': 'https://us-central1-%s.cloudfunctions.net/process-task' % config.PROJECT_ID,
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


def create_action(action, action_id, db, tasks_client, group_id=None, person_id=None):
    if not group_id and not person_id:
        logging.error('Missing group id and person id to create action')
        return
    action_doc = db.collection('persons').document(person_id if person_id else group_id) \
        .collection('actions').document(action_id)
    if action_doc.exists and 'task_id' in action_doc.to_dict():
        try:
            tasks_client.delete_task(name=action_doc.get('task_id'))
        except:
            logging.warning('Could not delete task {}'.format(action_doc.get('task_id')))

    task = {'action_id': action_id}
    if person_id:
        task['person_id'] = person_id
    else:
        task['group_id'] = group_id
    action['task_id'] = schedule_task(task, tasks_client)
    action_doc.reference.set(action)
