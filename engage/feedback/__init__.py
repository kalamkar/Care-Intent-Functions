import config
import logging

import croniter
import datetime
import pytz

from conversation import Conversation as BaseConversation

from google.cloud import bigquery

DATA_MESSAGES = {
    'medication': 'Did you take the medication?'
}


class Conversation(BaseConversation):
    def __init__(self, config, context):
        super().__init__(config, context)
        self.missing_tasks = []

    def can_process(self):
        if not self.is_scheduled_time():
            logging.info('Not scheduled time')
            return False
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now

        tasks = self.context.get('person.tasks')
        if not tasks or type(tasks) != dict:
            logging.info('No tasks for the person')
            return False
        for name, task in tasks.items():
            if 'data' not in task or 'schedule' not in task:
                continue
            if not self.has_completed(task['schedule'], task['data'], now):
                self.missing_tasks.append(task)

        logging.info('{} tasks found'.format(len(self.missing_tasks)))
        return len(self.missing_tasks) > 0

    def process(self):
        self.config['last_message_type'] = self.__module__ + '.task_confirm'
        messages = [DATA_MESSAGES[task['data']] if task['data'] in DATA_MESSAGES else 'Did you do it?'
                    for task in self.missing_tasks]
        self.reply = ' '.join(messages)

    def has_completed(self, schedule, data, now):
        cron = croniter.croniter(schedule, now)
        schedule_time = cron.get_prev(datetime.datetime)
        start_time = (schedule_time - datetime.timedelta(seconds=60 * 60)).isoformat()
        bq = bigquery.Client()
        q = 'SELECT count(*) as num_rows FROM careintent.live.tsdata, UNNEST(data) ' \
            'WHERE time > TIMESTAMP("{start}") AND name = "{name}"'
        q = q.format(project=config.PROJECT_ID, name=data, start=start_time)
        return list(bq.query(q))[0]['num_rows'] > 0
