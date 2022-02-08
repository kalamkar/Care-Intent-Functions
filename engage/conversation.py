import abc
import config
import croniter
import datetime
import json
import logging
import pytz

from google.cloud import pubsub_v1


class Conversation(abc.ABC):
    def __init__(self, config, context):
        self.transfer_type = None
        self.reply = None
        self.last_message_type = None
        self.config = config
        self.context = context

    @abc.abstractmethod
    def can_process(self):
        return False

    @abc.abstractmethod
    def process(self):
        pass

    def is_scheduled_time(self, tolerance_seconds=5*60):
        if 'schedule' not in self.config:
            return False
        now = datetime.datetime.utcnow()
        timezone = self.context.get('person.timezone')
        now = now.astimezone(pytz.timezone(timezone)) if timezone else now
        cron = croniter.croniter(self.config['schedule'], now)
        schedule_time = cron.get_prev(datetime.datetime)
        logging.info('Now {} and schedule time for {} is {}'.format(now, self.config['type'], schedule_time))
        return abs((now - schedule_time).total_seconds()) <= tolerance_seconds  # If reminder time is within few seconds

    def publish_data(self, source_id=None, params=None, content=None, tags=()):
        params = params if params else {}
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        if type(tags) == list:
            tags.append('source:action')
        elif type(tags) == str:
            tags = tags.split(',')

        if not params and content:
            params = json.loads(content, strict=False)

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': source_id,
            'tags': tags,
            'data': []
        }
        for name, value in params.items():
            if type(value) in [int, float]:
                row['data'].append({'name': name, 'number': value})
            elif type(value) == str:
                row['data'].append({'name': name, 'value': value})
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))
