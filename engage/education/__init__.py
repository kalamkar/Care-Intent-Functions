import logging

from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        return self.is_scheduled_now()

    def process(self):
        if 'message_id' not in self.config:
            logging.warning('Missing message_id for %s' % self.config)
            return

        messages = self.context.get('person.message_id', default='').split(',')
        if len(messages) > 1:
            message_index = self.context.get('person.message_index', -1) + 1
            message_index = message_index if 0 <= message_index < len(messages) else 0
            self.message_id = [messages[message_index]]
            self.context.set('person', {'message_index': message_index})
        elif messages:
            self.message_id = [messages[0]]
