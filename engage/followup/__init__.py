import config
import datetime
import logging
import pytz
import random

from dateutil import parser as dateparser
from conversation import Conversation as BaseConversation

from google.cloud import bigquery


class Conversation(BaseConversation):
    def can_process(self):
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now
        is_scheduled_now = self.is_scheduled_time(now)
        if is_scheduled_now and 'ended' in self.config:
            del self.config['ended']
        if is_scheduled_now and 'check' in self.config and self.config['check'] == 'tasks':
            is_missing_task = False
            tasks = self.context.get('person.tasks')
            if not tasks or type(tasks) != dict:
                logging.info('No tasks for the person')
                return False
            for name, task in tasks.items():
                if 'data' not in task or 'schedule' not in task:
                    continue
                last_completed_time = self.last_completed(self.context.get('person.id.value'), task['data'])
                last_completed_time = last_completed_time.astimezone(pytz.timezone(timezone))\
                    if timezone else last_completed_time
                if last_completed_time < (now - datetime.timedelta(hours=14)):
                    self.context.set('missing_task', task | {'last_completed_time': last_completed_time})
                    is_missing_task = True
            return is_missing_task
        elif is_scheduled_now and 'check' in self.config:
            return self.context.render(self.config['check']) == 'True'

        last_message_id = self.context.get('person.last_message_id')
        return last_message_id and last_message_id.startswith(self.__module__) and 'ended' not in self.config

    def process(self):
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now

        last_message_id = self.context.get('person.last_message_id')
        missing_task = self.context.get('missing_task')
        if self.config['check'] == 'tasks' and missing_task:
            task_type = missing_task['data'] if 'data' in missing_task else 'generic'
            if missing_task['last_completed_time'] < now - datetime.timedelta(hours=38):
                self.transfer_type = 'barriers'
            else:
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
        elif 'message_id' in self.config:
            self.message_id = [self.config['message_id']]
            self.config['ended'] = True
        elif 'message_ids' in self.config:
            messages = self.config['message_ids']
            self.message_id = [messages[random.randint(0, len(messages)-1)]]
            self.config['ended'] = True

    def last_completed(self, source, data):
        bq = bigquery.Client()
        q = 'SELECT time FROM careintent.live.tsdata, UNNEST(data) '\
            'WHERE source.value = "{source}" AND name = "{name}" '\
            'ORDER BY time DESC LIMIT 1'
        q = q.format(project=config.PROJECT_ID, name=data, source=source)
        rows = list(bq.query(q))
        return dateparser.parse(rows[0]['time']) if rows else datetime.datetime.utcfromtimestamp(0)
