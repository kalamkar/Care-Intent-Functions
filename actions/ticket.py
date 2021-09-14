import logging
import config
import datetime
import json

from google.cloud import bigquery
from google.cloud import pubsub_v1

from generic import Action


class List(Action):
    def process(self, person_id=None):
        if not person_id or 'value' not in person_id:
            logging.error('Missing person_id for list tickets')
            return

        tickets = {}
        bq = bigquery.Client()
        q = 'SELECT name, number, value FROM {project}.live.tsdata, UNNEST(data) ' \
            'WHERE source.value = "{source}" AND "ticket" IN UNNEST(tags) AND number IS NOT NULL ' \
            'ORDER BY time'.format(project=config.PROJECT_ID, source=person_id['value'])
        for row in bq.query(q):
            if row['number'] > 0:
                if row['name'] not in tickets:
                    tickets[row['name']] = []
                tickets[row['name']].append(row['value'])
            elif row['number'] < 0:
                if row['name'] not in tickets:
                    logging.error('Ticket {} for {} closed before opening'.format(row['name'], person_id))
                    continue
                if len(tickets[row['name']]) > 0:
                    tickets[row['name']].pop(0)
                if len(tickets[row['name']]) == 0:
                    del tickets[row['name']]
        self.context_update = {'tickets': tickets}


class Operation(Action):
    def __init__(self, number):
        super().__init__()
        self.number = number

    def process(self, person_id=None, name=None, message=None):
        if not name:
            logging.error('Missing name for open ticket')
            return
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': person_id,
            'tags': ['ticket'],
            'data': [{'name': name, 'number': self.number, 'value': message}]
        }
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


class Open(Operation):
    def __init__(self):
        super().__init__(1)


class Close(Operation):
    def __init__(self):
        super().__init__(-1)
