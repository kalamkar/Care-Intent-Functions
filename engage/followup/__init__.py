import common
import config
import croniter
import dateutil.parser
import datetime
import logging
import pytz

from conversation import Conversation as BaseConversation

from google.cloud import bigquery


class Conversation(BaseConversation):
    def can_process(self):
        is_scheduled_now = self.is_scheduled_now()
        last_message_id = self.context.get('person.last_message_id')

        if is_scheduled_now and 'ended' in self.config:
            del self.config['ended']

        if is_scheduled_now and 'repeat_question' in self.config:
            return True
        elif is_scheduled_now and 'check' in self.config and self.config['check'] == 'tasks':
            return self.is_missing_task()
        elif is_scheduled_now and 'check' in self.config:
            return self.is_missing_task()

        return last_message_id and last_message_id.startswith(self.__module__) and 'ended' not in self.config and \
               not self.is_scheduled_run()

    def is_missing_task(self):
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now

        is_missing_task = False
        tasks = self.context.get('person.tasks')
        if not tasks or type(tasks) != list:
            logging.info('No tasks for the person')
            return False
        for task in tasks:
            if 'data' not in task or 'schedule' not in task:
                continue
            last_completed_time = self.last_completed(self.context.get('person.id.value'), task['data'])
            last_completed_time = last_completed_time.astimezone(pytz.timezone(timezone)) \
                if timezone else last_completed_time
            self.config['last_completed_hours'] = int((now - last_completed_time).total_seconds() / 3600)
            last_expected_time = croniter.croniter(task['schedule'], now).get_prev(datetime.datetime)
            tolerance = datetime.timedelta(seconds=(3600 if 'tolerance' not in task else
                                                    common.get_duration_secs(task['tolerance'])))
            if last_completed_time < (last_expected_time - tolerance):
                self.context.set('missing_task', task)
                is_missing_task = True
        return is_missing_task

    def process(self):
        last_completed_hours = self.config['last_completed_hours'] if 'last_completed_hours' in self.config else 1000
        last_message_id = self.context.get('person.last_message_id')
        content_type = self.context.get('message.content_type')
        missing_task = self.context.get('missing_task')
        logging.info('Missing task {} last message id {}'.format(missing_task, last_message_id))
        if self.is_scheduled_now() and 'repeat_question' in self.config:
            if self.config['repeat_question']:
                self.skip_message_id_update = True
                self.message_id = list(last_message_id.split('.')[1:])
            del self.config['repeat_question']
        elif self.config['check'] == 'tasks' and missing_task:
            task_type = missing_task['data'] if 'data' in missing_task else 'generic'
            self.message_id = ['task_confirm', task_type]
            self.config['prev_message'] = 'task_confirm'
            if 'repeat' in self.config and self.config['repeat']:
                self.config['repeat_question'] = True
        elif not self.is_scheduled_now() and last_message_id and content_type != 'application/json' and\
                last_message_id.startswith(self.__module__ + '.task_confirm'):
            if 'repeat_question' in self.config:
                self.config['repeat_question'] = False
            df = self.detect_intent(contexts={'yes_no': {}})
            if df.query_result.intent.display_name == 'generic.yes':
                self.publish_data(source_id=self.context.get('person.id'), tags='biometrics',
                                  params={'time': '', 'medication': ''})
                self.message_id = ['task_confirm_yes', last_message_id.split('.')[-1]]
                self.config['ended'] = True
            elif df.query_result.intent.display_name == 'generic.no' and last_completed_hours > 40:
                self.is_missing_task()  # Set the missing task in the context for barrier module
                self.transfer_type = 'barriers'
            elif df.query_result.intent.display_name == 'generic.no':
                self.message_id = ['task_confirm_no']
                self.config['ended'] = True
            else:
                logging.warning('Unexpected result {}'.format(df.query_result))
                self.skip_message_id_update = True
                self.message_id = ['confirm_yes']
        elif self.is_scheduled_now() and 'message_id' in self.config:
            if ',' in self.config['message_id']:
                messages = self.config['message_id'].split(',')
                message_index = (self.config['message_index'] + 1) if 'message_index' in self.config else 0
                message_index = message_index if 0 <= message_index < len(messages) else 0
                self.message_id = [messages[message_index]]
                self.config['message_index'] = message_index
            else:
                self.message_id = [self.config['message_id']]
            self.config['ended'] = True

    def last_completed(self, source, data):
        bq = bigquery.Client()
        q = 'SELECT time FROM careintent.live.tsdata, UNNEST(data) '\
            'WHERE source.value = "{source}" AND name = "{name}" '\
            'ORDER BY time DESC LIMIT 1'
        q = q.format(project=config.PROJECT_ID, name=data, source=source)
        rows = list(bq.query(q))
        return rows[0]['time'] if rows else datetime.datetime.utcfromtimestamp(0)

    def is_scheduled_now(self):
        conversation = self.context.get('message.content.conversation')
        return 'schedule' in self.config and conversation and 'schedule' in conversation and \
               conversation['schedule'] == self.config['schedule']

    def is_scheduled_run(self):
        return self.context.get('message.content.conversation') is not None
