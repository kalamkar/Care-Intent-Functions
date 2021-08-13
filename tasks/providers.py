import datetime
import json

import requests


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
