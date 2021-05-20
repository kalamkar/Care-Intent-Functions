import config

ALLOW_HEADERS = 'Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Authorization, ' \
                'Cookie, Access-Control-Request-Method, Access-Control-Request-Headers, ' \
                'Access-Control-Allow-Credentials, Cache-Control, Pragma, Expires'


def make_response(request, methods='GET'):
    import flask
    response = flask.make_response()
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'

    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = methods
        response.headers['Access-Control-Allow-Headers'] = ALLOW_HEADERS
        response.status_code = 204

    return response


def create_dexcom_auth_url(person_id):
    from urllib.parse import urlencode
    import cipher
    return 'https://sandbox-api.dexcom.com/v2/oauth2/login?' + urlencode({
        'client_id': config.DEXCOM_ID,
        'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth',
        'response_type': 'code',
        'scope': 'offline_access',
        'state': cipher.create_auth_token({'person-id': person_id, 'provider': 'dexcom'})
    })


def create_dexcom_polling(payload, repeat_secs):
    from google.cloud import tasks_v2
    client = tasks_v2.CloudTasksClient()
    queue = client.queue_path('careintent', 'us-central1', 'dexcom')

    from google.protobuf import timestamp_pb2
    timestamp = timestamp_pb2.Timestamp()
    import datetime
    timestamp.FromDatetime(datetime.datetime.utcnow() + datetime.timedelta(seconds=repeat_secs))

    import json
    payload['repeat-secs'] = repeat_secs
    task = {
        'http_request': {  # Specify the type of request.
            'http_method': tasks_v2.HttpMethod.POST,
            'url': 'https://us-central1-careintent.cloudfunctions.net/process-task',
            'headers': {"Content-type": "application/json"},
            'body': json.dumps(payload).encode()
        },
        'schedule_time': timestamp
    }
    response = client.create_task(request={'parent': queue, 'task': task})
    print("Created task {}".format(response.name))
