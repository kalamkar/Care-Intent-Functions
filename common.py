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
