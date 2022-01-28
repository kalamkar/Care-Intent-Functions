from conversation import Conversation as BaseConversation

DATA_CONFIG = {
    'hip': {'label': 'Hip\'s circumference', 'low': 0, 'high': 0},
    'waist': {'label': 'Waist\'s circumference', 'low': 0, 'high': 0},
    'weight': {'label': 'Weight', 'low': 0, 'high': 0},
    'height': {'label': 'Height', 'low': 0, 'high': 0},
    'temperature': {'label': 'Temperature', 'low': 0, 'high': 0},
    'a1c': {'label': 'A1C (blood sugar level)', 'low': 0, 'high': 0},
    'diastolic': {'label': 'Blood pressure', 'low': 0, 'high': 0},
    'systolic': {'label': 'Blood pressure', 'low': 0, 'high': 0}
}


class Conversation(BaseConversation):
    def can_process(self):
        return self.context.get('message.nlp.intent').startswith("biomarker")

    def process(self):
        params = self.context.get('message.nlp.params')
        self.publish_data(source_id=self.context.get('person.id'), tags='biometrics', params=params)
        param_name = list(params.keys())[0]
        self.reply = 'Your {} has been recorded'.format(
            DATA_CONFIG[param_name]['label'].lower() if param_name in DATA_CONFIG else 'data')
        # self.config['last_message_type'] = 'q1'
