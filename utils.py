import config
import datetime
import json
import requests

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from urllib.parse import urlencode


def create_dexcom_polling(payload, repeat_secs):
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path('careintent', 'us-central1', 'dexcom')

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(datetime.datetime.utcnow() + datetime.timedelta(seconds=repeat_secs))

    payload['repeat-secs'] = repeat_secs
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': 'https://us-central1-careintent.cloudfunctions.net/process-task',
            'oidc_token': {'service_account_email': 'careintent@appspot.gserviceaccount.com'},
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        },
        'schedule_time': timestamp
    }
    response = client.create_task(request={'parent': queue, 'task': task})
    print("Created task {}".format(response.name))


def get_dexcom_egvs(access_token, last_sync):
    end = datetime.datetime.utcnow()
    start = (last_sync + datetime.timedelta(seconds=1)) if last_sync else (end - datetime.timedelta(days=90))
    url = 'https://sandbox-api.dexcom.com/v2/users/self/egvs?startDate=%s&endDate=%s'\
          % (start.strftime('%Y-%m-%dT%H:%M:%S'), end.strftime('%Y-%m-%dT%H:%M:%S'))
    print(url)
    response = requests.get(url, headers={'Authorization': 'Bearer ' + access_token})
    print(response.content)
    return response.json() if response.status_code == 200 else None


def get_dexcom_access(refresh):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = urlencode({
        'client_id': config.DEXCOM_ID,
        'client_secret': config.DEXCOM_SECRET,
        'refresh_token': refresh,
        'grant_type': 'refresh_token',
        'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'
    })
    response = requests.post('https://sandbox-api.dexcom.com/v2/oauth2/token', body, headers=headers)
    if response.status_code > 299:
        return None
    provider = response.json()
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    return provider
