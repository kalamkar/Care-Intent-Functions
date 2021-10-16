import base64
import cipher
import config
import datetime
import dateutil.parser
import json
import logging
import pytz
import requests
import uuid

from generic import Action

from google.cloud import pubsub_v1
from urllib.parse import urlencode
from google.cloud import firestore


class DataProvider(Action):
    def process(self, name=None, source_id=None, access_token=None, expires=None, refresh_token=None, last_sync=None):
        if not expires or expires < datetime.datetime.utcnow().astimezone(pytz.UTC):
            response = get_access_token(refresh_token, name)
            if 'access_token' not in response:
                logging.warning('Missing access token while renewing it.')
                logging.warning(response)
                return
            access_token = response['access_token']
            response['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=response['expires_in'])
            self.action_update.update(response)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')
        local_last_sync = last_sync
        for row in PROVIDERS[name](access_token, local_last_sync, source_id):
            row_time = dateutil.parser.parse(row['time']).astimezone(pytz.UTC)
            local_last_sync = max(row_time, local_last_sync) if local_last_sync else row_time
            publisher.publish(topic_path, json.dumps(row).encode('utf-8'))

        if not last_sync or (local_last_sync and last_sync < local_last_sync):
            self.action_update['last_sync'] = local_last_sync


def get_dexcom_data(access_token, last_sync, source_id):
    end = datetime.datetime.utcnow()
    start = (last_sync + datetime.timedelta(seconds=1)) if last_sync else (end - datetime.timedelta(days=7))
    url = 'https://sandbox-api.dexcom.com/v2/users/self/egvs?startDate=%s&endDate=%s'\
          % (start.strftime('%Y-%m-%dT%H:%M:%S'), end.strftime('%Y-%m-%dT%H:%M:%S'))
    logging.info(url)
    response = requests.get(url, headers={'Authorization': 'Bearer ' + access_token})
    logging.info(response.content)
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
    logging.info(response.content)
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
        'redirect_uri': 'https://{}-{}.cloudfunctions.net/auth'.format(config.LOCATION_ID, config.PROJECT_ID)
    })
    response = requests.post(config.PROVIDERS[provider_name]['url'], body, headers=headers)
    if response.status_code > 299:
        logging.info(response.content)
        return response.json()
    return response.json()


PROVIDERS = {'dexcom': get_dexcom_data, 'google': get_google_data}


def create_dexcom_auth_url(person_id):
    return 'https://sandbox-api.dexcom.com/v2/oauth2/login?' + urlencode({
        'client_id': config.DEXCOM_ID,
        'redirect_uri': 'https://{}-{}.cloudfunctions.net/auth'.format(config.LOCATION_ID, config.PROJECT_ID),
        'response_type': 'code',
        'scope': 'offline_access',
        'state': cipher.create_auth_token(
            {'person_id': person_id, 'action_id': 'dexcom', 'schedule': '0-55/5 * * * *'})
    })


def create_google_auth_url(person_id):
    return 'https://accounts.google.com/o/oauth2/v2/auth?' + urlencode({
        'prompt': 'consent',
        'response_type': 'code',
        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
        'scope': 'https://www.googleapis.com/auth/fitness.activity.read',
        'access_type': 'offline',
        'redirect_uri': 'https://{}-{}.cloudfunctions.net/auth'.format(config.LOCATION_ID, config.PROJECT_ID),
        'state': cipher.create_auth_token(
            {'person_id': person_id, 'action_id': 'google', 'schedule': '0 * * * *'})
    })


PROVIDER_URLS = {'dexcom': create_dexcom_auth_url,
                 'google': create_google_auth_url}


class OAuth(Action):
    def __init__(self, person_id=None, provider=None):
        self.person_id = person_id
        self.provider = provider
        super().__init__()

    def process(self):
        short_code = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')
        db = firestore.Client()
        db.collection('urls').document(short_code).set({
            'redirect': PROVIDER_URLS[self.provider](self.person_id)
        })
        self.context_update['oauth'] = {'url': 'https://u.careintent.com/{}'.format(short_code)}
