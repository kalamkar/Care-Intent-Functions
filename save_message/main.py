import base64
import config
import datetime
import json
import logging

from google.cloud import bigquery

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(data)
    logging.info(message)

    content = message['content'] if 'content' in message else None
    if content and type(content) != str:
        content = json.dumps(content)
    row = {
        'time': message['time'] if 'time' in message else datetime.datetime.utcnow().isoformat(),
        'status': message['status'] if 'status' in message else None,
        'content_type': message['content_type'] if 'content_type' in message else None,
        'content': content,
        'tags': message['tags'] if 'tags' in message and message['tags'] else []
    }
    if 'sender' in message and message['sender'] and 'type' in message['sender']:
        row['sender'] = {'type': message['sender']['type'], 'value': message['sender']['value']}
    if 'receiver' in message and message['receiver'] and 'type' in message['receiver']:
        row['receiver'] = {'type': message['receiver']['type'], 'value': message['receiver']['value']}

    client = bigquery.Client()
    table_id = '%s.live.messages' % config.PROJECT_ID
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        logging.error(errors)
