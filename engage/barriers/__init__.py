from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        return False

    def process(self):
        missing_task = self.context.get('missing_task')
        if missing_task:
            task_type = missing_task['data'] if 'data' in missing_task else 'generic'
            self.message_id = ['which', task_type]
