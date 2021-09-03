import argparse
import croniter
import datetime
import json
import pytz
import sys
import traceback

from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

PROJECT_ID = 'careintent'
PROJECT_LOCATION = 'us-central1'


def delete_task(task_id):
    try:
        client = tasks_v2.CloudTasksClient()
        # queue = client.queue_path(PROJECT_ID, PROJECT_LOCATION, 'actions')
        client.delete_task(name=task_id)
    except:
        traceback.print_exc()


def schedule_task(payload, next_run_time):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path(PROJECT_ID, PROJECT_LOCATION, 'actions')
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(next_run_time)
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': 'https://%s-%s.cloudfunctions.net/process-task' % (PROJECT_LOCATION, PROJECT_ID),
            'oidc_token': {'service_account_email': 'careintent@appspot.gserviceaccount.com'},
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        },
        'schedule_time': timestamp
    }
    response = client.create_task(request={'parent': queue, 'task': task})
    print("Created task {}".format(response.name))
    return response.name


def main(argv):
    parser = argparse.ArgumentParser(description='Add or replace actions for a group.')
    parser.add_argument('--group', help='Id of the group for the given actions.', required=True)
    parser.add_argument('--clean', help='Delete all the existing actions for the group.', default=False,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('--delete_prefix', help='Delete actions with given prefix of action id.')
    parser.add_argument('--files', help='List of files to read actions from.', type=argparse.FileType('r'), nargs='+')
    args = parser.parse_args(argv)

    db = firestore.Client()
    collection = db.collection('groups').document(args.group).collection('actions')
    if args.clean:
        for action in collection.stream():
            if 'task_id' in action.to_dict():
                delete_task(action.get('task_id'))
            action.reference.delete()
    elif args.delete_prefix:
        for action in collection.stream():
            if not action.id.startswith(args.delete_prefix):
                continue
            if 'task_id' in action.to_dict():
                delete_task(action.get('task_id'))
            action.reference.delete()

    if not args.files:
        return
    for file in args.files:
        for action in json.load(file)['actions']:
            if 'schedule' in action:
                action_doc = collection.document(action['id']).get()
                if action_doc.exists and action_doc.get('task_id'):
                    delete_task(action.get('task_id'))
                payload = {'action_id': action['id'], 'group_id': args.group}
                now = datetime.datetime.utcnow()
                now = now.astimezone(pytz.timezone(action['timezone'])) if 'timezone' in action else now
                cron = croniter.croniter(action['schedule'], now)
                action['task_id'] = schedule_task(payload, cron.get_next(datetime.datetime))
            collection.document(action['id']).set(action)


if __name__ == '__main__':
    main(sys.argv[1:])
