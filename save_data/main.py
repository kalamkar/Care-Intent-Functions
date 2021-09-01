import base64
import json

from google.cloud import bigquery

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
        Args:
             event (dict): Event payload.
             context (google.cloud.functions.Context): Metadata for the event.
        """

    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    print(data)

    client = bigquery.Client()
    table_id = '%s.live.tsdata' % PROJECT_ID
    errors = client.insert_rows_json(table_id, [data])
    if errors:
        print(errors)
