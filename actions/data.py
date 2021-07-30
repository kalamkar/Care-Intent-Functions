ACTIONS = [{
    'priority': 1,
    'type': 'Update',
    'condition': '$message.dialogflow.intent == "system.welcome" and $sender.name is None',
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
    'condition': '$message.dialogflow.intent == "food.end"',
    'id': '2yULpJaMPlzCt7O5wLVW'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'content': '{"dialogflow": {"context": {"name": "food_report", "lifespan": 5}}}',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'condition': '$data.pattern == "slope"',
    'id': '5IcqdTjPalz1E310eKiF'
}, {
    'priority': 1,
    'params': {
        'sender': '$message.receiver',
        'content': '$dialogflow.fulfillment-text',
        'receiver': '$message.sender'
    },
    'condition': 're.match("^smalltalk.*$", $message.dialogflow.action)',
    'type': 'Message',
    'id': 'LCUPdM6q5tFV2rdu0Ufx'
},{
    'type': 'Message',
    'params': {
        'content': 'What did you eat?',
        'receiver': '$sender'
    },
    'priority': 1,
    'condition': '$data.pattern == "slope"',
    'id': 'LPRTZpIjwEp0DHYhGFDC'
}, {
    'condition': '$message.dialogflow.intent == "system.welcome" and $sender.name is not None',
    'priority': 1,
    'params': {
        'receiver': '$message.sender',
        'sender': '$message.receiver',
        'content': '$dialogflow.fulfillment-text, $sender.name.first'
    },
    'type': 'Message',
    'id': 'M3Fmlhjv4Az1d4vn5XDs'
}, {
    'condition': '$message.dialogflow.action == "connect.dexcom"',
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
    'condition': '$data.source.type == "dexcom"',
    'priority': 2,
    'type': 'SimplePatternCheck',
    'hold_secs': 3600,
    'id': 'UjclBjOUF88VnnhBPnzF'
}, {
    'condition': '$message.dialogflow.intent == "system.welcome" and $sender.name is None',
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
    'condition': '$message.dialogflow.action == "food.report"',
    'id': 'aZ4Oc5gspNy85fntVzJh'
}, {
    'type': 'OAuthMessage',
    'params': {
        'provider': 'google',
        'person_id': '$sender.id',
        'receiver': '$message.sender',
        'sender': '$message.receiver'
    },
    'condition': '$message.dialogflow.action == "connect.google"',
    'priority': 1,
    'id': 'ffWxDX4rv37InPAuro05'
}, {
    'type': 'DataExtract',
    'params': {
        'params': '$message.dialogflow.params',
        'person_id': '$sender.id'
    },
    'condition': '$message.dialogflow.intent == "biomarker.report.bp"',
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
    'condition': 're.match("food.*", $message.dialogflow.intent)',
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
    'condition': 're.match("medication.*", $message.dialogflow.intent)',
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
    'condition': 're.match("activity.*", $message.dialogflow.intent)',
    'id': 'WQXODyp9Q-q0tZZDGvHV0Q'
}, {
    'type': 'Update',
    'priority': 1,
    'params': {
        'list_name': 'topics',
        'content': '["biomarker"]',
        'identifier': '$sender.id',
        'collection': 'persons'
    },
    'condition': 're.match("biomarker.*", $message.dialogflow.intent)',
    'id': 'skNVAa3cTkaLw9BpFVg__Q'
}]
