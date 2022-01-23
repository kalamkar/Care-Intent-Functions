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
            schedule_time = cron.get_prev(datetime.datetime)
            return (now - schedule_time).total_seconds() <= 5  # If schedule time is within few seconds
        return False

    def process(self):
        self.reply = self.config['message'] if 'message' in self.config else 'Did you do it?'
        self.config['last_message_type'] = 'q1'
