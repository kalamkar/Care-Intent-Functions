from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        last_message_id = self.context.get('person.last_message_id')
        return last_message_id and last_message_id.startswith(self.__module__) and 'ended' not in self.config

    def process(self):
        last_message_id = self.context.get('person.last_message_id')
        missing_task = self.context.get('missing_task')
        if missing_task:
            task_type = missing_task['data'] if 'data' in missing_task else 'generic'
            self.message_id = ['which', task_type]
        elif last_message_id and '.which.' in last_message_id:
            df = self.detect_intent(contexts={'challenge-identification': {}})
            if 'fallback' not in df.query_result.intent.display_name:
                self.config['barrier'] = df.query_result.intent.display_name
                self.reply = df.query_result.fulfillment_text
                self.message_id = ['barrier', df.query_result.intent.display_name]
            else:
                self.message_id = ['explain']
        elif last_message_id and '.barrier.' in last_message_id:
            self.config['ended'] = True
            self.message_id = ['ok']
