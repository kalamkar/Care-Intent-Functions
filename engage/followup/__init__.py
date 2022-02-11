import config
import logging

import croniter
import datetime
import pytz

from conversation import Conversation as BaseConversation

from google.cloud import bigquery


class Conversation(BaseConversation):
    def __init__(self, config, context):
        super().__init__(config, context)
        self.missing_tasks = []

    def can_process(self):
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now
        is_scheduled_now = self.is_scheduled_time(now)
        if is_scheduled_now and 'ended' in self.config:
            del self.config['ended']
        if is_scheduled_now and 'check' in self.config and self.config['check'] == 'tasks':
            tasks = self.context.get('person.tasks')
            if not tasks or type(tasks) != dict:
                logging.info('No tasks for the person')
                return False
            for name, task in tasks.items():
                if 'data' not in task or 'schedule' not in task:
                    continue
                if not self.has_completed(task['schedule'], task['data'], now):
                    self.missing_tasks.append(task)
            return len(self.missing_tasks) > 0
        elif is_scheduled_now and 'check' in self.config:
            return self.context.render(self.config['check']) == 'True'

        last_message_id = self.context.get('person.last_message_id')
        return last_message_id and last_message_id.startswith(self.__module__) and 'ended' not in self.config

    def process(self):
        last_message_id = self.context.get('person.last_message_id')
        if self.config['check'] == 'tasks' and self.missing_tasks:
            task_type = self.missing_tasks[0]['data'] if self.missing_tasks else 'generic'
            self.message_id = ['task_confirm', task_type]
        elif last_message_id and last_message_id.startswith(self.__module__ + '.task_confirm'):
            df = self.detect_intent(contexts={'yes_no': {}})
            if df.query_result.intent.display_name == 'generic.yes':
                self.publish_data(source_id=self.context.get('person.id'), tags='biometrics',
                                  params={'time': '', 'medication': ''})
                self.message_id = ['task_confirm_yes', last_message_id.split('.')[-1]]
                self.config['ended'] = True
            elif df.query_result.intent.display_name == 'generic.no':
                self.message_id = ['task_confirm_no']
                self.config['ended'] = True
            else:
                logging.warning('Unexpected result {}'.format(df.query_result))
                self.skip_message_id_update = True
                self.message_id = ['confirm_yes']
        else:
            self.message_id = [self.config['message_id']]
            self.config['ended'] = True

    def has_completed(self, schedule, data, now):
        cron = croniter.croniter(schedule, now)
        schedule_time = cron.get_prev(datetime.datetime)
        start_time = (schedule_time - datetime.timedelta(seconds=60 * 60)).isoformat()
        bq = bigquery.Client()
        q = 'SELECT count(*) as num_rows FROM careintent.live.tsdata, UNNEST(data) ' \
            'WHERE time > TIMESTAMP("{start}") AND name = "{name}"'
        q = q.format(project=config.PROJECT_ID, name=data, start=start_time)
        return list(bq.query(q))[0]['num_rows'] > 0
