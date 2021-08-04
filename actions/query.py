from google.cloud import firestore

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


def get_relations(relation_types, actor_id, source=True):
    db = firestore.Client()
    relations = db.collection('relations').where('source' if source else 'target', '==', actor_id)
    if relation_types:
        relations = relations.where('type', 'in', relation_types)
    return list(relations.get())


def get_relatives(source_id, relation_types, target_id, max_degree=1):
    if source_id and target_id:
        return []
    relatives = []
    keys = [(source_id, 0)] if source_id else [(target_id, 0)]
    while keys:
        key, degree = keys.pop()
        if degree >= max_degree:
            continue
        for relation in get_relations(relation_types, key, True if source_id else False):
            relative = relation.get('target') if source_id else relation.get('source')
            if relative not in relatives:
                relatives.append(relative)
            keys.append((relative, degree + 1))
    return relatives
