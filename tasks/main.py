import datetime
import dexcom
import json
import pytz
import dateutil.parser

from google.cloud import firestore
from google.cloud import pubsub_v1

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def handle_task(request):
    print(request.json)

    db = firestore.Client()
    person_ref = db.collection('persons').document(request.json['person-id'])
    provider_ref = person_ref.collection('providers').document(request.json['provider'])
    provider = provider_ref.get().to_dict()
    if 'expires' not in provider or \
            provider['expires'] < datetime.datetime.utcnow().astimezone(pytz.UTC):
        provider = dexcom.get_dexcom_access(provider['refresh_token'])
    last_sync = provider['last_sync'] if 'last_sync' in provider else None
    data = dexcom.get_dexcom_egvs(provider['access_token'], last_sync)
    if data:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, 'data')
        latest = None
        for reading in data['egvs']:
            row = {
                'time': reading['systemTime'],
                'source': {'type': 'dexcom', 'id': person_ref.id},
                'data': []
            }
            for k, v in reading.items():
                if k not in ['systemTime', 'displayTime']:
                    row['data'].append({'name': k, 'value' if type(v) == str else 'number': v})
            row_time = dateutil.parser.parse(row['time'])
            latest = max(row_time, latest) if latest else row_time
            publisher.publish(topic_path, json.dumps(row).encode('utf-8'))

        if latest:
            provider['last_sync'] = latest

    provider_ref.set(provider)

    if 'repeat-secs' in request.json:
        dexcom.create_dexcom_polling(request.json, request.json['repeat-secs'])

    return 'OK'
