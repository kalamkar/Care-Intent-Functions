import cipher
import datetime
import dexcom
import flask
import requests

from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': dexcom.DEXCOM_ID,
                        'client_secret': dexcom.DEXCOM_SECRET},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}


def handle_auth(request):
    state = request.args.get('state')
    if state:
        provider = 'dexcom'
        state = cipher.parse_auth_token(state)
    else:
        provider = 'google'

    data = {'client_id': PROVIDERS[provider]['client_id'],
            'client_secret': PROVIDERS[provider]['client_secret'],
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    response = requests.post(PROVIDERS[provider]['url'], data=data)
    print(response.content)

    if provider == 'dexcom':
        db = firestore.Client()
        person_ref = db.collection('persons').document(state['person-id'])
        provider_ref = person_ref.collection('providers').document(state['provider'])
        provider = response.json()
        provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
        provider_ref.set(provider)

        dexcom.create_dexcom_polling(state, 5 * 60)

    return flask.redirect('https://www.careintent.com', 302)
