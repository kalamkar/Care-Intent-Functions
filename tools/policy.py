import argparse
import common
import croniter
import datetime
import json
import logging
import pytz
import sys

from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

from csv2actions import csv2actions
from survey2actions import csv2actions as survey_csv2actions


PROJECT_ID = 'careintent'
PROJECT_LOCATION = 'us-central1'


def delete_task(task_id):
    try:
        client = tasks_v2.CloudTasksClient()
        print('Deleting task %s' % task_id)
        client.delete_task(name=task_id)
    except:
        logging.error('Error deleting task', exc_info=sys.exc_info())


def main(argv):
    parser = argparse.ArgumentParser(description='Add or replace actions for a group.')
    parser.add_argument('--policy', help='Id of the policy for the given policy file.')
    parser.add_argument('--group', help='Id of the group for the scheduled actions.')
    parser.add_argument('--delete_prefix', help='Delete scheduled actions with given prefix of action id.')
    parser.add_argument('--json', help='JSON file to read policy from.', type=argparse.FileType('r'))
    parser.add_argument('--csv', help='CSV file to read policy from.', type=argparse.FileType('r'))
    parser.add_argument('--survey', help='CSV survey file to read policy from.', type=argparse.FileType('r'))
    parser.add_argument('--save', help='JSON file to write policy actions to.', type=argparse.FileType('w'))
    args = parser.parse_args(argv)

    actions = []
    if args.json:
        actions = json.load(args.json)['actions']
    elif args.csv:
        actions = csv2actions(args.csv)
    elif args.survey and args.policy:
        actions = survey_csv2actions(args.policy, args.csv)

    if args.save:
        json.dump({'actions': actions}, args.save, indent=2)

    len_scheduled = len(list(filter(lambda a: 'schedule' in a, actions)))
    if 0 < len_scheduled < len(actions):
        # Either none or all should be schedule actions
        logging.error('Mixed scheduled and reactive actions')
        return

    db = firestore.Client()
    if len_scheduled == 0 and args.policy:
        db.collection('policies').document(args.policy).set({action['id']: action for action in actions})
    elif len_scheduled != 0 and args.group:
        collection = db.collection('groups').document(args.group).collection('actions')
        for action in actions:
            action_doc = collection.document(action['id']).get()
            if action_doc.exists and 'task_id' in action_doc.to_dict():
                delete_task(action_doc.get('task_id'))
            payload = {'action_id': action['id'], 'target_id': {'type': 'group', 'value': args.group},
                       'child_type': action['child_type'] if 'child_type' in action else 'member'}
            now = datetime.datetime.utcnow()
            now = now.astimezone(pytz.timezone(action['timezone'])) if 'timezone' in action else now
            cron = croniter.croniter(action['schedule'], now)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(cron.get_next(datetime.datetime))
            action['task_id'] = common.schedule_task(payload, tasks_v2.CloudTasksClient(), timestamp=timestamp)
            print('Scheduled task %s' % action['task_id'])

            collection.document(action['id']).set(action)
            print('Added action %s' % action['id'])
    elif args.delete_prefix:
        for action in db.collection('groups').document(args.group).collection('actions').stream():
            if not action.id.startswith(args.delete_prefix):
                continue
            if 'task_id' in action.to_dict():
                delete_task(action.get('task_id'))
            print('Deleting action %s' % action.id)
            action.reference.delete()
    else:
        parser.print_help()


if __name__ == '__main__':
    main(sys.argv[1:])
