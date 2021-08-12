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