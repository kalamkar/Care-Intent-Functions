from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        last_message_id = self.context.get('person.last_message_id')
        return last_message_id and last_message_id.startswith(self.__module__) and 'ended' not in self.config

    def process(self):
        missing_task = self.context.get('missing_task')
        if missing_task:
            task_type = missing_task['data'] if 'data' in missing_task else 'generic'
            self.message_id = ['which', task_type]
            if 'ended' in self.config:
                del self.config['ended']
        elif 'generic_input' in self.config and self.config['generic_input']:
            self.config['ended'] = True
            self.message_id = ['generic_input_reply']
            self.add_barrier(self.config['barrier'])
            del self.config['generic_input']
        else:
            df = self.detect_intent(contexts={'challenge-identification': {}})
            if 'fallback' in df.query_result.intent.display_name:
                self.message_id = ['explain']
            else:
                self.config['barrier'] = df.query_result.intent.display_name
                self.reply = df.query_result.fulfillment_text
                self.message_id = ['barrier', df.query_result.intent.display_name]
                self.add_barrier(df.query_result.intent.display_name)

            if df.query_result.action == 'end_conversation':
                self.config['ended'] = True
            elif df.query_result.action == 'generic_input':
                self.config['generic_input'] = True

    def add_barrier(self, name, value=None):
        if 'barriers' not in self.config:
            self.config['barriers'] = []
        self.config['barriers'].append({'type': name,
                                        'content': value if value else self.context.get('message.content')})
