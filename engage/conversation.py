import abc
import config
import datetime
import json

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
