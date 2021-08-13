import base64
import datetime
import json

from google.cloud import bigquery

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def save_message(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)

    row = {
        'time': message['time'] if 'time' in message else datetime.datetime.utcnow().isoformat(),
        'status': message['status'] if 'status' in message else None,
        'content_type': message['content_type'] if 'content_type' in message else None,
        'content': message['content'] if 'content' in message else None,
        'tags': message['tags'] if 'tags' in message else []
    }
    if 'sender' in message and message['sender'] and 'type' in message['sender']:
        row['sender'] = {'type': message['sender']['type'], 'value': message['sender']['value']}
    if 'receiver' in message and message['receiver'] and 'type' in message['receiver']:
        row['receiver'] = {'type': message['receiver']['type'], 'value': message['receiver']['value']}
    if 'dialogflow' in message and message['dialogflow']:
        if 'intent' in message['dialogflow']:
            row['tags'].append(message['dialogflow']['intent'])
        if 'action' in message['dialogflow']:
            row['tags'].append(message['dialogflow']['action'])

    client = bigquery.Client()
    table_id = '%s.live.messages' % PROJECT_ID
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        print(errors)
