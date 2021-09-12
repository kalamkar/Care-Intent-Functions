
PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
SYSTEM_GROUP_ID = 'system'

LOCATION_ID = 'us-central1'
DEXCOM_ID = 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg'

SYSTEM_KNOWLEDGE_ID = 'Nzg3NTUyMTQxNDI5NDQwNTEyMA'

PHONE_NUMBER = '+16692154466'
EMAIL_ADDRESS = 'support@careintent.com'

PROXY_PHONE_NUMBERS = ['+13166130001', '+13166130002', '+13166130003', '+13166130004', '+13166130005']

SENDGRID_TOKEN = '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'

SESSION_SECONDS = 3 * 60 * 60
GAP_SECONDS = 20 * 60

PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg',
                        'client_secret': 'NZ4sTh0n4X6AT0XE'},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}
