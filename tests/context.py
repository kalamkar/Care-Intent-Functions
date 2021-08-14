import unittest
from actions.context import Context


class ContextTestCase(unittest.TestCase):
    def testSet(self):
        context = Context()
        context.set('sender', {'id': 'sender_id'})
        self.assertEqual('sender_id', context.get('sender.id'))
        context.set('sender', {'name': 'sender1'})
        self.assertEqual('sender_id', context.get('sender.id'))
        self.assertEqual('sender1', context.get('sender.name'))

    def testSetMessage(self):
        context = Context()
        context.set('message', {'time': '2021-08-13T02:58:41.007415',
                                'sender': {'type': 'phone', 'value': '+13233376800'},
                                'receiver': {'type': 'phone', 'value': '+16692154466'},
                                'status': 'received', 'tags': [], 'content_type': 'text/plain',
                                'content': 'hi',
                                'dialogflow': {'intent': 'system.welcome', 'action': 'input.welcome',
                                               'reply': 'Greetings! How can I assist?', 'confidence': 100,
                                               'params': {}}})
        self.assertEqual('hi', context.get('message.content'))
        context.clear('message')
        self.assertEqual(None, context.get('message.content'))
        context.clear('foo')
