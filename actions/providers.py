import config
import datetime
import dateutil.parser
import json
import pytz
import requests

from generic import Action

from google.cloud import pubsub_v1
from urllib.parse import urlencode


class DataProvider(Action):
    def __init__(self, name=None, source_id=None, access_token=None, expires=None, refresh_token=None, last_sync=None):
        super().__init__()
        self.name = name
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expiration = expires
        self.last_sync = last_sync
        self.source_id = source_id

    def process(self):
        if not self.expiration or self.expiration < datetime.datetime.utcnow().astimezone(pytz.UTC):
            response = get_access_token(self.refresh_token, 'google')
            self.access_token = response['access_token']
            self.expiration = datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
            response['expires'] = self.expiration
            self.action_update.update(response)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')
        last_sync = self.last_sync
        for row in PROVIDERS[self.name](self.access_token, last_sync, self.source_id):
            row_time = dateutil.parser.parse(row['time']).astimezone(pytz.UTC)
            last_sync = max(row_time, last_sync) if last_sync else row_time
            publisher.publish(topic_path, json.dumps(row).encode('utf-8'))

        if not self.last_sync or (last_sync and self.last_sync < last_sync):
            self.action_update['last_sync'] = last_sync


def get_dexcom_data(access_token, last_sync, source_id):
    end = datetime.datetime.utcnow()
    start = (last_sync + datetime.timedelta(seconds=1)) if last_sync else (end - datetime.timedelta(days=7))
    url = 'https://sandbox-api.dexcom.com/v2/users/self/egvs?startDate=%s&endDate=%s'\
          % (start.strftime('%Y-%m-%dT%H:%M:%S'), end.strftime('%Y-%m-%dT%H:%M:%S'))
    print(url)
    response = requests.get(url, headers={'Authorization': 'Bearer ' + access_token})
    print(response.content)
    if response.status_code > 299 or not response.content or 'egvs' not in response.json():
        return []

    egvs = []
    for reading in response.json()['egvs']:
        egvs.append({
            'time': reading['systemTime'],
            'source': source_id,
            'data': [{'name': 'glucose', 'number': reading['value']},
                     {'name': 'trend', 'value': reading['trend']},
                     {'name': 'trendRate', 'number': reading['trendRate']}],
            'tags': ['dexcom']
        })
    return egvs


def get_google_data(access_token, last_sync, source_id):
    end = datetime.datetime.utcnow()
    start = (last_sync + datetime.timedelta(seconds=1)) if last_sync else (end - datetime.timedelta(days=7))
    headers = {'Authorization': 'Bearer ' + access_token,
               'Content-type': 'application/json'}
    url = 'https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate'
    body = json.dumps({
        'aggregateBy': [{
            'dataTypeName': 'com.google.step_count.delta',
            'dataSourceId': 'derived:com.google.step_count.delta:com.google.android.gms:estimated_steps'
        }],
        'bucketByTime': {'durationMillis': 60 * 60 * 1000},  # 60 mins
        'startTimeMillis': int(start.strftime('%s')) * 1000,
        'endTimeMillis': int(end.strftime('%s')) * 1000
    })
    response = requests.post(url, body, headers=headers)
    print(response.content)
    if response.status_code > 299 or not response.content or 'bucket' not in response.json():
        return []

    rows = []
    for bucket in response.json()['bucket']:
        points = bucket['dataset'][0]['point'] if len(bucket['dataset']) > 0 else []
        if len(points) > 0 and 'startTimeNanos' in points[0] and 'value' in points[0]\
                and len(points[0]['value']) > 0 and 'intVal' in points[0]['value'][0]\
                and 'dataTypeName' in points[0] and points[0]['dataTypeName'] == 'com.google.step_count.delta':
            start = datetime.datetime.utcfromtimestamp(int(points[0]['startTimeNanos']) / 1000000000)
            end = datetime.datetime.utcfromtimestamp(int(points[0]['endTimeNanos']) / 1000000000)
            duration = end - start
            rows.append({
                'time': start.isoformat(),
                'duration': duration.total_seconds(),
                'source': source_id,
                'data': [{'name': 'steps', 'number': points[0]['value'][0]['intVal']}],
                'tags': ['gfit']
        })
    return rows


def get_access_token(refresh, provider_name):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = urlencode({
        'client_id': config.PROVIDERS[provider_name]['client_id'],
        'client_secret': config.PROVIDERS[provider_name]['client_secret'],
        'refresh_token': refresh,
        'grant_type': 'refresh_token',
        'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'
    })
    response = requests.post(config.PROVIDERS[provider_name]['url'], body, headers=headers)
    if response.status_code > 299:
        print(response.content)
        return response.json()
    return response.json()


PROVIDERS = {'dexcom': get_dexcom_data, 'google': get_google_data}
