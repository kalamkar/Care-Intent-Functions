import abc
import config
import croniter
import datetime
import json
import logging
import random

from google.cloud import dialogflow_v2beta1 as dialogflow
from google.cloud import pubsub_v1

from messages import DATA as messages


class Conversation(abc.ABC):
    def __init__(self, config, context):
        self.transfer_type = None
        self.message_id = []
        self.skip_message_id_update = False
        self.reply = None
        self.config = config
        self.context = context

    @abc.abstractmethod
    def can_process(self):
        return False

    @abc.abstractmethod
    def process(self):
        pass

    def is_scheduled_time(self, now, tolerance_seconds=30):
        if 'schedule' not in self.config:
            return False
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

    def get_reply(self):
        if self.reply:
            return self.context.render(self.reply)
        for name in [self.__module__ + '.' + '.'.join(self.message_id[:n]) for n in range(len(self.message_id), 0, -1)]:
            if name in messages:
                message = messages[name] if type(messages[name]) == str\
                    else messages[name][random.randint(0, len(messages[name])-1)]
                return self.context.render(message)
        return self.context.render(messages[self.__module__]) if not self.transfer_type else ''

    def detect_intent(self, content=None, contexts=None):
        if not content:
            content = self.context.get('message.content')
        session_id = self.context.get('person.id.value')
        query_params = dialogflow.types.QueryParameters(
            {'contexts': [build_df_context(session_id, name, data=value) for name, value in contexts.items()]
            if contexts else None})
        df_client = dialogflow.SessionsClient()
        text_input = dialogflow.types.TextInput({'text': content[:255], 'language_code': 'en-US'})
        request = {
            'session': df_client.session_path(config.PROJECT_ID, session_id),
            'query_input': dialogflow.types.QueryInput({'text': text_input}),
            'query_params': query_params
        }
        response = df_client.detect_intent(request)
        logging.info('DF request {} and response {}'.format(request, response))
        return response


def build_df_context(session_id, name, data):
    df_context = dialogflow.types.Context({
        'name': 'projects/{project}/agent/sessions/{session}/contexts/{name}'
            .format(project=config.PROJECT_ID, session=session_id, name=name),
        'lifespan_count': data['lifespanCount'] if 'lifespanCount' in data else 1
    })
    if 'parameters' in data:
        df_context.parameters.update(data['parameters'])
    return df_context
