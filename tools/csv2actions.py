import argparse
import csv
import json
import logging
import sys

ACTION_TYPES = ['Broadcast', 'CreateAction', 'DataProvider', 'Message', 'OAuth', 'RunAction', 'OpenAI', 'DialogFlow',
                'UpdateContext', 'UpdateData', 'UpdateRelation', 'UpdateResource', 'ListGroup', 'QueryData',
                'OpenTicket', 'CloseTicket', 'ListTickets', 'ListMessages', 'Webhook']

DURATIONS = {
    's': 1,
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
        elif name == 'for':
            action['child_type'] = value
        elif name == 'actionhold':
            action['hold_secs'] = get_duration_secs(value)
        elif name == 'msgselect':
            action['content_select'] = value
        elif name == 'priority':
            action['priority'] = int(value)
        elif name == 'min_action_priority':
            action['min_action_priority'] = int(value)
        elif name == 'tokens':
            action['tokens'] = int(value)
        elif name == 'temperature':
            action['temperature'] = int(value)
        elif name == 'delay':
            action['params']['delay_secs'] = get_duration_secs(value)
        elif name == 'period':
            action['params']['period'] = get_duration_secs(value)
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
        elif name == 'ticket_priority':
            action['params']['priority'] = int(value)
        else:
            action['params'][name] = value
    return action


def csv2actions(file):
    actions = []
    reader = csv.DictReader(file)
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
                'priority': 10,
                'params': {}
            }
            action.update(create_action(row['Params']))

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

        if row['Message'].strip():
            if 'content' not in action['params']:
                action['params']['content'] = []
            if type(action['params']['content']) == list:
                action['params']['content'].append({
                    'id': row['Variation Id'] or '1',
                    'message': row['Message'].strip()
                })
    if action:
        actions.append(action)
    return actions


def main(argv):
    parser = argparse.ArgumentParser(description='Add or replace actions for a group.')
    parser.add_argument('--file', help='CSV file to read policy from.', type=argparse.FileType('r'), required=True)
    args = parser.parse_args(argv)
    actions = csv2actions(args.file)
    json.dump({'actions': actions}, open(args.file.name + '.json', 'w'), indent=2)


if __name__ == '__main__':
    main(sys.argv[1:])
