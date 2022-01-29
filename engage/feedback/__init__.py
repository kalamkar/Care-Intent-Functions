import croniter
import datetime
import pytz

from conversation import Conversation as BaseConversation

DATA_MESSAGES = {
    'medication': 'Did you take the medication?'
}


class Conversation(BaseConversation):
    def __init__(self, config, context):
        super().__init__(config, context)
        self.missing_tasks = []

    def can_process(self):
        if not self.is_scheduled_time():
            return False
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now

        for task in self.context.get('person.tasks'):
            if 'data' not in task:
                continue
            if not self.has_completed(task, now):
                self.missing_tasks.append(task)

        return len(self.missing_tasks) > 0

    def process(self):
        self.config['last_message_type'] = self.__module__ + '.task_confirm'
        messages = [DATA_MESSAGES[task['data']] if task['data'] in DATA_MESSAGES else 'Did you do it?'
                    for task in self.missing_tasks]
        self.reply = ' '.join(messages)

    def has_completed(self, task, now):
        cron = croniter.croniter(task['schedule'], now)
        schedule_time = cron.get_prev(datetime.datetime)
        query_time = schedule_time - datetime.timedelta(seconds=60 * 60)
        # TODO: Query bq for data report here
        return False

