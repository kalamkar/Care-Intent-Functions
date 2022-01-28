import croniter
import datetime
import pytz

from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        if 'schedule' in self.config:
            now = datetime.datetime.utcnow()
            now = now.astimezone(pytz.timezone(self.config['timezone'])) if 'timezone' in self.config else now
            cron = croniter.croniter(self.config['schedule'], now)
            reminder_time = cron.get_prev(datetime.datetime)
            return (now - reminder_time).total_seconds() <= 5  # If reminder time is within few seconds
        return False

    def process(self):
        if 'message' in self.config:
            self.reply = self.config['message']
            return
        self.reply = 'This is a reminder'
#        self.reply = self.config['message'] if 'message' in self.config else 'Did you do it?'
#        self.config['last_message_type'] = 'q1'

