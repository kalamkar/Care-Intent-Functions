import abc


class Conversation(abc.ABC):
    def __init__(self, config, context):
        self.transfer_type = None
        self.reply = None
        self.config = config
        self.context = context

    @abc.abstractmethod
    def process(self):
        pass
