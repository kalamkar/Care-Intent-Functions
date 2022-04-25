import logging

from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        return self.is_scheduled_now()

    def process(self):
        if 'message_id' not in self.config:
            logging.warning('Missing message_id for %s' % self.config)
            return

        if ',' in self.config['message_id']:
            messages = self.config['message_id'].split(',')
            message_index = (self.config['message_index'] + 1) if 'message_index' in self.config else 0
            message_index = message_index if 0 <= message_index < len(messages) else 0
            self.message_id = [messages[message_index]]
            self.config['message_index'] = message_index
        else:
            self.message_id = [self.config['message_id']]
