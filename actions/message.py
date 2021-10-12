import common
import config
import datetime
import json
import logging
import pytz

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1

from generic import Action


class Send(Action):
    def process(self, receiver=None, sender=None, content=None, tags=None):
        if type(tags) == list:
            tags.append('source:action')
        elif type(tags) == str:
            tags = tags.split(',') + ['source:action']
        else:
            tags = ['source:action']

        db = firestore.Client()
        if sender and type(sender) == dict and 'type' in sender and sender['type'] == 'person' and\
                receiver and type(receiver) == dict and 'type' in receiver and receiver['type'] == 'person':
            sender = common.get_proxy_id(receiver, sender, db)
            tags.append('proxy')
        if receiver and type(receiver) == dict and 'type' in receiver and receiver['type'] == 'person':
            person_doc = db.collection(common.COLLECTIONS[receiver['type']]).document(receiver['value']).get()
            person = person_doc.to_dict()
            if 'stopped' in person:
                logging.error('Skipping message to person who has unsubscribed messages.')
                return
            now = datetime.datetime.utcnow().astimezone(pytz.utc)
            if common.is_valid_session(person):
                tags.append('session:' + person['session']['id'])
                person_doc.reference.update({'session.last_message_time': now})
            else:
                person_doc.reference.update({'session': {'start': now, 'id': common.generate_id(),
                                                         'last_message_time': now}})
        sender = common.get_identifier(sender, 'phone', db,
                                       {'type': 'phone', 'value': config.PHONE_NUMBER}, ['group'])
        receiver = common.get_identifier(receiver, 'phone', db)
        if not receiver:
            logging.warning('Missing receiver for message action')
            return
        if sender == receiver or receiver['value'] in [config.PHONE_NUMBER] + config.PROXY_PHONE_NUMBERS:
            logging.error('Sending message to system phone numbers to {r} from {s}'.format(r=receiver, s=sender))
            return

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        data = {
            'time': datetime.datetime.utcnow().isoformat(),
            'sender': sender,
            'receiver': receiver,
            'status': 'sent',
            'tags': tags,
            'content_type': 'text/plain',
            'content': content
        }
        publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')


class Broadcast(Action):
    def process(self, parent_id=None, content=None, tags=None):
        if type(tags) == list:
            tags.append('source:action')
        elif type(tags) == str:
            tags = tags.split(',') + ['source:action']
        else:
            tags = ['source:action']

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, 'message')

        db = firestore.Client()
        sender = common.get_identifier(parent_id, 'phone', db,
                                       {'type': 'phone', 'value': config.PHONE_NUMBER}, ['group'])
        for member_doc in db.collection(common.COLLECTIONS[parent_id['type']]).document(parent_id['value'])\
                .collection('members').stream():
            member = member_doc.to_dict()
            if 'stopped' in member:
                logging.error('Skipping message to person who has unsubscribed messages.')
                continue
            now = datetime.datetime.utcnow().astimezone(pytz.utc)
            if common.is_valid_session(member):
                tags.append('session:' + member['session']['id'])
                member_doc.reference.update({'session.last_message_time': now})
            else:
                member_doc.reference.update({'session': {'start': now, 'id': common.generate_id(),
                                                         'last_message_time': now}})

            receiver = common.filter_identifier(member_doc, 'phone')
            if not receiver:
                logging.warning('Missing receiver for broadcast action, member {}'.format(member_doc.id))
                continue

            data = {
                'time': datetime.datetime.utcnow().isoformat(),
                'sender': sender,
                'receiver': receiver,
                'status': 'sent',
                'tags': tags,
                'content_type': 'text/plain',
                'content': content
            }
            publisher.publish(topic_path, json.dumps(data).encode('utf-8'), send='true')


class List(Action):
    def process(self, sender_id=None, receiver_id=None, period=12*60*60, tag=None, limit=None):
        senders = []
        receivers = []
        db = firestore.Client()
        if sender_id:
            sender = db.collection(common.COLLECTIONS[sender_id['type']]).document(sender_id['value']).get()
            senders.extend([i['value'] for i in sender.get('identifiers')])
        if receiver_id:
            receiver = db.collection(common.COLLECTIONS[receiver_id['type']]).document(receiver_id['value']).get()
            receivers.extend([i['value'] for i in receiver.get('identifiers')])
        if sender_id and receiver_id:
            proxy_id = common.get_proxy_id(sender_id, receiver_id, db)  # sender is parent
            if proxy_id:
                receivers.append(proxy_id['value'])
            proxy_id = common.get_proxy_id(receiver_id, sender_id, db)  # receiver is parent
            if proxy_id:
                senders.append(proxy_id['value'])
        q = 'SELECT time, status, sender, receiver, tags, content, content_type ' +\
            'FROM {project}.live.messages WHERE ' +\
            'time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {period} second) ' + \
            ('AND sender.value IN ("{senders}") ' if sender_id else '') + \
            ('AND receiver.value IN ("{receivers}") ' if receiver_id else '') +\
            'AND "{tag}" IN UNNEST(tags) ' + \
            'ORDER BY time' + (' DESC LIMIT {lmt}' if limit else '')
        q = q.format(project=config.PROJECT_ID, period=period, tag=tag, lmt=limit,
                     senders='","'.join(senders), receivers='","'.join(receivers))
        logging.info(q)
        bq = bigquery.Client()
        rows = []
        for row in bq.query(q):
            rows.append({'time': row['time'].isoformat(),
                         'status': row['status'],
                         'sender': row['sender'],
                         'receiver': row['receiver'],
                         'tags': row['tags'],
                         'content': row['content'],
                         'content_type': row['content_type']})
        self.context_update = {'messages': rows}