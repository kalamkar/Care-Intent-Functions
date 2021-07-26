ACTIONS = [{
    'priority': 1,
    'type': 'Update',
    'rules': [
        {'name': 'message.dialogflow.intent', 'weight': 50, 'value': 'system.welcome', 'compare': 'str'},
        {'compare': 'isnull', 'weight': 50, 'name': 'sender.name'}
    ],
    'params': {
        'collection': 'persons',
        'content': '{"dialogflow": {"context": {"name": "setup_name", "lifespan": 1}}}',
        'identifier': '$sender.id'
    },
    'id': '04rnCmZCWwwk0aohQaZJ'
}, {
    'params': {
        'content': '$dialogflow.fulfillment-text',
        'receiver': '$message.sender',
        'sender': '$message.receiver'
    },
    'priority': 1,
    'type': 'Message',
    'rules': [{'name': 'message.dialogflow.intent', 'weight': 100, 'compare': 'str', 'value': 'food.end'}],
    'id': '2yULpJaMPlzCt7O5wLVW'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'content': '{"dialogflow": {"context": {"name": "food_report", "lifespan": 5}}}',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'rules': [{'compare': 'str', 'weight': 100, 'value': 'slope', 'name': 'data.pattern'}],
    'id': '5IcqdTjPalz1E310eKiF'
}, {
    'priority': 1,
    'params': {
        'sender': '$message.receiver',
        'content': '$dialogflow.fulfillment-text',
        'receiver': '$message.sender'
    },
    'rules': [{'name': 'message.dialogflow.action', 'weight': 100, 'compare': 'regex', 'value': '^smalltalk.*$'}],
    'type': 'Message',
    'id': 'LCUPdM6q5tFV2rdu0Ufx'
},{
    'type': 'Message',
    'params': {
        'content': 'What did you eat?',
        'receiver': '$sender'
    },
    'priority': 1,
    'rules': [{
        'name': 'data.pattern', 'compare': 'str', 'value': 'slope', 'weight': 100
    }],
    'id': 'LPRTZpIjwEp0DHYhGFDC'
}, {
    'rules': [
        {'value': 'system.welcome', 'weight': 50, 'compare': 'str', 'name': 'message.dialogflow.intent'},
        {'weight': 50, 'compare': 'notnull', 'name': 'sender.name'}
    ],
    'priority': 1,
    'params': {
        'receiver': '$message.sender',
        'sender': '$message.receiver',
        'content': '$dialogflow.fulfillment-text'
    },
    'type': 'Message',
    'id': 'M3Fmlhjv4Az1d4vn5XDs'
}, {
    'rules': [{'name': 'message.dialogflow.action', 'compare': 'str', 'weight': 100, 'value': 'connect.dexcom'}],
    'params': {
        'sender': '$message.receiver',
        'receiver': '$message.sender',
        'person_id': '$sender.id',
        'provider': 'dexcom'
    },
    'priority': 1,
    'type': 'OAuthMessage',
    'id': 'Qx5DH1cjtQIZUJJbwzyB'
}, {
    'params': {'person_id': '$data.source.id', 'max_threshold': 30, 'seconds': 14400, 'name': 'glucose'},
    'rules': [{'compare': 'str', 'name': 'data.source.type', 'weight': 100, 'value': 'dexcom'}],
    'priority': 2,
    'type': 'SimplePatternCheck',
    'repeat-secs': 3600,
    'id': 'UjclBjOUF88VnnhBPnzF'
}, {
    'rules': [
        {'name': 'message.dialogflow.intent', 'value': 'system.welcome', 'weight': 50, 'compare': 'str'},
        {'compare': 'isnull', 'name': 'sender.name', 'weight': 50}
    ],
    'params': {
        'content': '$dialogflow.fulfillment-text I am Ezra, what is your name?',
        'receiver': '$message.sender',
        'sender': '$message.receiver'},
    'priority': 1,
    'type': 'Message',
    'id': 'XtUYsSJEWCCuJlQYE8Wr'
}, {
    'priority': 1,
    'params': {
        'content': '$dialogflow.fulfillment-text',
        'sender': '$message.receiver',
        'receiver': '$message.sender'
    },
    'type': 'Message',
    'rules': [{'compare': 'str', 'name': 'message.dialogflow.action', 'value': 'food.report', 'weight': 100}],
    'id': 'aZ4Oc5gspNy85fntVzJh'
}, {
    'type': 'OAuthMessage',
    'params': {
        'provider': 'google',
        'person_id': '$sender.id',
        'receiver': '$message.sender',
        'sender': '$message.receiver'
    },
    'rules': [
        {'weight': 100, 'name': 'message.dialogflow.action', 'value': 'connect.google', 'compare': 'str'}
    ],
    'priority': 1,
    'id': 'ffWxDX4rv37InPAuro05'
}, {
    'type': 'DataExtract',
    'params': {
        'params': '$message.dialogflow.params',
        'person_id': '$sender.id'
    },
    'rules': [
        {'weight': 100, 'name': 'message.dialogflow.intent', 'value': 'biomarker.report.bp', 'compare': 'str'}
    ],
    'priority': 1,
    'id': 'zmrG5CweQGygxi9NPmbJmQ'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'list_name': 'topics',
        'content': '["food"]',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'rules': [{'compare': 'regex', 'weight': 100, 'value': 'food.*', 'name': 'message.dialogflow.intent'}],
    'id': 'oVSGH4sNSQeNTs-X24Xwaw'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'list_name': 'topics',
        'content': '["medication"]',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'rules': [{'compare': 'regex', 'weight': 100, 'value': 'medication.*', 'name': 'message.dialogflow.intent'}],
    'id': 'O6MRdvl6R_28yOYEduaX7g'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'list_name': 'topics',
        'content': '["activity"]',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'rules': [{'compare': 'regex', 'weight': 100, 'value': 'activity.*', 'name': 'message.dialogflow.intent'}],
    'id': 'WQXODyp9Q-q0tZZDGvHV0Q'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'list_name': 'topics',
        'content': '["bp"]',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'rules': [{'compare': 'regex', 'weight': 100, 'value': 'biomarker.*bp', 'name': 'message.dialogflow.intent'}],
    'id': 'skNVAa3cTkaLw9BpFVg__Q'
}]
