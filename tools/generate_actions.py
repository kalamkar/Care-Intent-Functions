import csv
import json
import logging
import sys

ACTION_TYPES = ['CreateAction', 'DataProvider', 'DelayRun', 'Message', 'OAuth', 'UpdateContext', 'UpdateData',
                'UpdateRelation', 'UpdateResource', 'OpenTicket', 'CloseTicket', 'ListTickets', 'Webhook']

DURATIONS = {
    'm': 60,
    'h': 60 * 60,
    'd': 24 * 60 * 60,
    'w': 7 * 24 * 60 * 60,
}


def get_duration_secs(duration):
    if duration[-1] not in DURATIONS:
        return 0
    return int(DURATIONS[duration[-1]] * float(duration[:-1]))


def create_action(params):
    action = {'params': {}}
    for param in params.split(';'):
        if ':' not in param:
            continue
        name, value = param.strip().split(':', 1)
        name, value = name.strip(), value.strip()
        if name == 'eval':
            action['condition'] = value
        elif name == 'schedule':
            action['schedule'] = value
        elif name == 'tz':
            action['timezone'] = value
        elif name == 'actionhold':
            action['hold_secs'] = get_duration_secs(value)
        elif name == 'msgselect':
            action['content_select'] = value
        elif name == 'priority':
            action['priority'] = int(value)
        elif name == 'min_action_priority':
            action['min_action_priority'] = int(value)
        elif name == 'delay':
            action['params']['delay_secs'] = get_duration_secs(value)
        elif name == 'new_priority':
            action['params']['priority'] = int(value)
        elif name == 'new_eval':
            action['params']['condition'] = value
        elif name == 'new_schedule':
            action['params']['schedule'] = value
        elif name == 'new_tz':
            action['params']['timezone'] = value
        elif name == 'new_actionhold':
            action['params']['hold_secs'] = get_duration_secs(value)
        else:
            action['params'][name] = value
    return action


def main(filename):
    actions = []
    reader = csv.DictReader(open(filename))
    action = None
    previous_id = None
    count = 0
    for row in reader:
        count += 1
        if row['Action type'] not in ACTION_TYPES:
            logging.error('Invalid action type %s' % row['Action type'])
            continue
        if row['Action Id'] != previous_id:
            previous_id = row['Action Id']
            if action:
                actions.append(action)
            action = {
                'id': row['Action Id'],
                'type': row['Action type'],
                'condition': 'False',  # In case of missing eval, default disabled rule
                'priority': 10
            }
            action.update(create_action(row['Params']))

        if action['type'] not in ['Message', 'CreateAction']:
            continue

        if action['type'] == 'Message' and 'receiver' not in action['params']:
            action['params']['receiver'] = '$person.id'
        if action['type'] == 'CreateAction':
            if 'parent_id' not in action['params']:
                action['params']['parent_id'] = '$person.id'
            if 'action_type' not in action['params']:
                action['params']['action_type'] = 'Message'
            if 'action' not in action['params']:
                action['params']['action'] = '$action'
            if 'maxrun' not in action['params']:
                action['params']['maxrun'] = 1
            if action['params']['action_type'] == 'Message' and 'receiver' not in action['params']:
                action['params']['receiver'] = '$person.id'

        if 'content' not in action['params']:
            action['params']['content'] = []
        if type(action['params']['content']) == list:
            action['params']['content'].append({
                'id': row['Variation Id'] or '1',
                'message': row['Message']
            })
            if row['Qualifiers']:
                try:
                    qualifiers = {q.split('=', 1)[0]: q.split('=', 1)[1] for q in row['Qualifiers'].split(';')}
                    action['params']['content'][-1]['qualifiers'] = qualifiers
                except:
                    print(count, row['Qualifiers'])
    if action:
        actions.append(action)
    json.dump({'actions': actions}, open(filename + '.json', 'w'), indent=2)


if __name__ == '__main__':
    main(sys.argv[1])
