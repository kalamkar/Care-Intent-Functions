import datetime
import requests


def get_dexcom_data(access_token, last_sync, source_id):
    end = datetime.datetime.utcnow()
    start = (last_sync + datetime.timedelta(seconds=1)) if last_sync else (end - datetime.timedelta(days=90))
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
            'source': {'type': 'dexcom', 'id': source_id},
            'data': [{'name': 'glucose', 'number': reading['value']},
                     {'name': 'trend', 'value': reading['trend']},
                     {'name': 'trendRate', 'number': reading['trendRate']}]
        })
    return egvs


def get_google_data(access_token, last_sync, source_id):
    return []
