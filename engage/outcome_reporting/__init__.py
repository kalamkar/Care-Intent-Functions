from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_start(self):
        return False

    def process(self):
        pass
