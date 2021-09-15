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
        q = 'SELECT time, id, status, category, title FROM (' \
            'SELECT time, ' \
            '(select number FROM UNNEST(data) WHERE name = "id") as id, '\
            '(select value FROM UNNEST(data) WHERE name = "id") as status, '\
            '(select value FROM UNNEST(data) WHERE name = "category") as category, '\
            '(select value FROM UNNEST(data) WHERE name = "title") as title '\
            'FROM {project}.live.tsdata '\
            'WHERE source.value = "{source}" AND "ticket" IN UNNEST(tags)) '\
            'ORDER BY time'.format(project=config.PROJECT_ID, source=person_id['value'])
        for row in bq.query(q):
            if row['status'] == 'opened':
                tickets[row['id']] = {'time': row['time'], 'category': row['category'], 'title': row['title']}
            elif row['status'] == 'closed':
                if row['id'] not in tickets:
                    logging.error('Ticket {} for {} closed before opening'.format(row['id'], person_id))
                    continue
                del tickets[row['id']]
        self.context_update = {'tickets': tickets}


class Operation(Action):
    def __init__(self, status):
        super().__init__()
        self.status = status

    def process(self, person_id=None, category=None, title=None):
        if not category or not person_id:
            logging.error('Missing name or person_id for opening ticket')
            return
        rows = bigquery.Client().query(
            'SELECT count(*) as count FROM {project}.live.tsdata, UNNEST(data) '\
            'WHERE source.value = "{source}" AND "ticket" IN UNNEST(tags) AND value = "opened" '\
            .format(project=config.PROJECT_ID, source=person_id['value']))
        ticket_id = rows[0]['count'] + 1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'data')

        row = {
            'time': datetime.datetime.utcnow().isoformat(),
            'source': person_id,
            'tags': ['ticket'],
            'data': [{'name': 'id', 'number': ticket_id, 'value': self.status}]
        }
        if category:
            row['data'].append({'name': 'category', 'value': category})
        if title:
            row['data'].append({'name': 'title', 'value': title})
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


class Open(Operation):
    def __init__(self):
        super().__init__('opened')


class Close(Operation):
    def __init__(self):
        super().__init__('closed')
