import base64
import config
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

    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    logging.info(data)

    client = bigquery.Client()
    table_id = '%s.live.tsdata' % config.PROJECT_ID
    errors = client.insert_rows_json(table_id, [data])
    if errors:
        logging.error(errors)
