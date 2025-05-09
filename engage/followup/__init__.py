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
        elif is_scheduled_now:
            return self.is_missing_task()

        return last_message_id and last_message_id.startswith(self.__module__ + '.task_confirm')\
               and 'ended' not in self.config and not self.is_scheduled_run()

    def is_missing_task(self):
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now

        task = self.get_recent_task(now)
        if not task:
            return False

        last_completed_time = self.last_completed(self.context.get('person.id.value'), task['data'])
        last_completed_time = last_completed_time.astimezone(pytz.timezone(timezone)) \
            if timezone else last_completed_time
        self.config['last_completed_hours'] = int((now - last_completed_time).total_seconds() / 3600)
        last_expected_time = croniter.croniter(task['schedule'], now).get_prev(datetime.datetime)
        window_start_time = last_expected_time - datetime.timedelta(seconds=(4 * 3600))
        window_end_time = last_expected_time + datetime.timedelta(seconds=(6 * 3600))
        logging.info('Is missing %s, expected %s, last completed %s' % (task, last_expected_time, last_completed_time))
        if not (window_start_time < last_completed_time < window_end_time):
            part_of_day = 'today'
            if last_expected_time.hour < 12:
                part_of_day = 'this morning'
            elif last_expected_time.hour >= 18:
                part_of_day = 'last night'
            task['part_of_day'] = part_of_day
            self.context.set('missing_task', task)
            return True
        return False

    def get_recent_task(self, now):
        tasks = self.context.get('person.tasks')
        if not tasks or type(tasks) != list:
            logging.info('No tasks for the person')
            return None
        timezone = self.context.get('person.timezone')
        latest_expected_time = datetime.datetime.utcfromtimestamp(0).astimezone(pytz.timezone(timezone)
                                                                                if timezone else pytz.UTC)
        latest_task = None
        for task in tasks:
            if 'data' not in task or 'schedule' not in task:
                continue
            last_expected_time = croniter.croniter(task['schedule'], now - datetime.timedelta(minutes=1))\
                .get_prev(datetime.datetime)
            if last_expected_time > latest_expected_time:
                latest_expected_time = last_expected_time
                latest_task = task
        return latest_task

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
                messages = self.context.get('person.message_id', default='').split(',')
                if messages:
                    message_index = self.context.get('person.message_index', -1) + 1
                    message_index = message_index if 0 <= message_index < len(messages) else 0
                    self.message_id = [messages[message_index]]
                    self.context.set('person', {'message_index': message_index})
                else:
                    self.message_id = ['task_confirm_no']
                self.config['ended'] = True
            else:
                logging.warning('Unexpected result {}'.format(df.query_result))
                self.skip_message_id_update = True
                self.message_id = ['confirm_yes']
        elif self.is_scheduled_now() and self.context.get('person.message_id'):
            messages = self.context.get('person.message_id', default='').split(',')
            if len(messages) > 1:
                message_index = self.context.get('person.message_index', -1) + 1
                message_index = message_index if 0 <= message_index < len(messages) else 0
                self.message_id = [messages[message_index]]
                self.context.set('person', {'message_index': message_index})
            elif messages:
                self.message_id = [messages[0]]
            self.config['ended'] = True

    def last_completed(self, source, data):
        bq = bigquery.Client()
        q = 'SELECT time FROM careintent.live.tsdata, UNNEST(data) '\
            'WHERE source.value = "{source}" AND name = "{name}" '\
            'ORDER BY time DESC LIMIT 1'
        q = q.format(project=config.PROJECT_ID, name=data, source=source)
        rows = list(bq.query(q))
        return rows[0]['time'] if rows else datetime.datetime.utcfromtimestamp(0)
