import common
import config
import datetime
import json
import logging
import numpy as np

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1

from generic import Action


class List(Action):
    def process(self, person_id=None, parent_id=None):
        if (not person_id or 'value' not in person_id) and (not parent_id or 'value' not in parent_id):
            logging.error('Missing person_id or parent_id for list tickets')
            return

        db = firestore.Client()
        sources = []
        if person_id:
            sources.append(person_id['value'])
        elif parent_id:
            sources.extend([pid['value'] for pid in common.get_children_ids(parent_id, 'member', db)])

        tickets = self.get_tickets_from_top_persons(self.get_open_tickets(sources)) if parent_id \
            else self.get_top_tickets_from_person(self.get_open_tickets(sources), sources[0])
        self.context_update = {'tickets': tickets}

    @staticmethod
    def get_top_tickets_from_person(tickets, person):
        return sorted(list(tickets[person].values()), key=lambda t: t['priority'], reverse=True)

    @staticmethod
    def get_tickets_from_top_persons(open_tickets):
        person_scores = {person: np.median([t['priority'] for t in tickets.values()]) if len(tickets) else 0
                         for person, tickets in open_tickets.items()}
        persons = [person for person, score in sorted(person_scores.items(), key=lambda kv: kv[1], reverse=True)]
        top_tickets = []
        for person in persons:
            tickets = sorted(open_tickets[person].values(), key=lambda t: t['priority'], reverse=True)
            if tickets:
                top_tickets.append(tickets[0])
        return top_tickets

    @staticmethod
    def get_open_tickets(sources):
        bq = bigquery.Client()
        q = 'SELECT time, source, id, priority, status, category, title FROM (' \
            'SELECT time, source, ' \
            '(select number FROM UNNEST(data) WHERE name = "id") as id, '\
            '(select value FROM UNNEST(data) WHERE name = "id") as status, '\
            '(select number FROM UNNEST(data) WHERE name = "priority") as priority, ' \
            '(select value FROM UNNEST(data) WHERE name = "category") as category, '\
            '(select value FROM UNNEST(data) WHERE name = "title") as title ' \
            'FROM {project}.live.tsdata '\
            'WHERE source.value IN ("{sources}") AND "ticket" IN UNNEST(tags)) '\
            'ORDER BY time'.format(project=config.PROJECT_ID, sources='","'.join(sources))
        tickets = {person: {} for person in sources}
        for row in bq.query(q):
            person = row['source']['value']
            ticket_id = int(row['id'])
            if row['status'] == 'opened':
                tickets[person][ticket_id] = {'person_id': row['source'], 'id': ticket_id, 'title': row['title'],
                                              'priority': row['priority'] if row['priority'] else 0,
                                              'time': row['time'].isoformat(), 'category': row['category']}
            elif row['status'] == 'closed':
                if ticket_id not in tickets[person]:
                    logging.error('Ticket {} for {} closed before opening'.format(ticket_id, person))
                    continue
                del tickets[person][ticket_id]
        return tickets


class Operation(Action):
    def __init__(self, status):
        super().__init__()
        self.status = status

    def process(self, person_id=None, ticket_id=None, category=None, content=None, priority=1):
        if not person_id:
            logging.error('Missing person_id for ticket operation')
            return
        if self.status == 'opened':
            rows = bigquery.Client().query(
                'SELECT count(*) as count FROM {project}.live.tsdata, UNNEST(data) '\
                'WHERE source.value = "{source}" AND "ticket" IN UNNEST(tags) AND value = "opened" '\
                .format(project=config.PROJECT_ID, source=person_id['value'])).result()
            ticket_id = list(rows)[0]['count'] + 1
        elif not ticket_id:
            logging.error('No ticket id provided for closing ticket')
            return

        if ticket_id and type(ticket_id) == str:
            ticket_id = int(ticket_id)

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
        if content:
            row['data'].append({'name': 'title', 'value': content})
        if self.status == 'opened':
            row['data'].append({'name': 'priority', 'number': priority})
        publisher.publish(topic_path, json.dumps(row).encode('utf-8'))


class Open(Operation):
    def __init__(self):
        super().__init__('opened')


class Close(Operation):
    def __init__(self):
        super().__init__('closed')
