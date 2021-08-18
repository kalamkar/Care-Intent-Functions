
PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
SYSTEM_GROUP_ID = 'system'

LOCATION_ID = 'us-central1'
DEXCOM_ID = 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg'

PHONE_NUMBER = '+16692154466'

PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg',
                        'client_secret': 'NZ4sTh0n4X6AT0XE'},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}
