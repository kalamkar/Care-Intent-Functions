import re

from conversation import Conversation as BaseConversation

INTENTS = [r'^biomarker', r'^medication\.report']


class Conversation(BaseConversation):
    def can_process(self):
        intent = self.context.get('message.nlp.intent')
        return intent is not None and re.search('|'.join(INTENTS), intent) is not None

    def process(self):
        params = self.context.get('message.nlp.params')
        self.publish_data(source_id=self.context.get('person.id'), tags='biometrics', params=params)
        param_name = sorted(list(params.keys()))[0]
        self.message_id = ['recorded', param_name]
        self.reply = self.get_reply()
        if 'message_id' in self.config:
            messages = self.config['message_id'].split(',')
            message_index = (self.config['message_index'] + 1) if 'message_index' in self.config else 0
            message_index = message_index if 0 <= message_index < len(messages) else 0
            self.message_id = [messages[message_index]]
            self.config['message_index'] = message_index
            self.reply += ' ' + self.get_reply()
