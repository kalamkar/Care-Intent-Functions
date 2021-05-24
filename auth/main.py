import cipher
import datetime
import dexcom
import flask
import requests

from google.cloud import firestore

PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7


def handle_auth(request):
    state = cipher.parse_auth_token(request.args.get('state'))

    data = {'client_id': dexcom.DEXCOM_ID,
            'client_secret': dexcom.DEXCOM_SECRET,
            'code': request.args.get('code'),
            'grant_type': 'authorization_code',
            'redirect_uri': 'https://us-central1-careintent.cloudfunctions.net/auth'}
    response = requests.post('https://sandbox-api.dexcom.com/v2/oauth2/token', data=data)
    print(response.content)

    db = firestore.Client()
    person_ref = db.collection('persons').document(state['person-id'])
    provider_ref = person_ref.collection('providers').document(state['provider'])
    provider = response.json()
    provider['expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=provider['expires_in'])
    provider_ref.set(provider)

    dexcom.create_dexcom_polling(state, 5 * 60)

    return flask.redirect('https://www.careintent.com', 302)
