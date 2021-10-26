
PROJECT_ID = 'careintent'  # os.environ.get('GCP_PROJECT')  # Only for py3.7
SYSTEM_GROUP_ID = 'system'

LOCATION_ID = 'us-central1'
DEXCOM_ID = 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg'

SYSTEM_KNOWLEDGE_ID = 'Nzg3NTUyMTQxNDI5NDQwNTEyMA'

PHONE_NUMBER = '+18446000211'
EMAIL_ADDRESS = 'support@careintent.com'

# Used in actions and receive functions
PROXY_PHONE_NUMBERS = ['+13166130001', '+13166130002', '+13166130003', '+13166130004', '+13166130005', '+13166130006',
                       '+13166130009', '+13166130010', '+13166130013', '+13166130015', '+13166130020', '+13166130023',
                       '+13166130027', '+13166130028', '+13166130030', '+13166130031', '+13166130032', '+13166130034',
                       '+13166130043', '+13166130045', '+13166130046', '+13166130047', '+13166130049', '+13166130053',
                       '+13166130054', '+13166130055', '+13166130058', '+13166130059', '+13166130060', '+13166130063',
                       '+13166130064', '+13166130065', '+13166130071', '+13166130072', '+13166130073', '+13166130074',
                       '+13166130077', '+13166130079', '+13166130083', '+13166130088', '+13166130089', '+13166130092',
                       '+13166130095', '+13166130097']

SENDGRID_TOKEN = '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'

GAP_SECONDS = 20 * 60

PROVIDERS = {'dexcom': {'url': 'https://sandbox-api.dexcom.com/v2/oauth2/token',
                        'client_id': 'cfz2ttzaLK164vTJ3lkt02n7ih0YMBHg',
                        'client_secret': 'NZ4sTh0n4X6AT0XE'},
             'google': {'url': 'https://oauth2.googleapis.com/token',
                        'client_id': '749186156527-hl1f7u9o2cssle1n80nl09bej2bjfg97.apps.googleusercontent.com',
                        'client_secret': 'GnBZGO7unmlgmko2CwqgRbBk'}}
