from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        return type(self.context.get('message.content')) == str

    def process(self):
        self.reply = self.detect_intent().query_result.fulfillment_text
