import base64
import json

from google.cloud import bigquery

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def save_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
             event (dict): Event payload.
             context (google.cloud.functions.Context): Metadata for the event.
        """

    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    print(data)

    client = bigquery.Client()
    table_id = '%s.live.data' % PROJECT_ID
    errors = client.insert_rows_json(table_id, [data])
    if errors:
        print(errors)
