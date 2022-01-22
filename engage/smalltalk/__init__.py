import config
import dialogflow_v2beta1 as dialogflow

from conversation import Conversation as BaseConversation


class Conversation(BaseConversation):
    def can_process(self):
        return True

    def process(self):
        person_id = self.context.get('person.id.value')
        content = self.context.get('message.content')
        query_params = dialogflow.types.QueryParameters()
        df_client = dialogflow.SessionsClient()
        text_input = dialogflow.types.TextInput(text=content[:255], language_code='en-US')
        df = df_client.detect_intent(session=df_client.session_path(config.PROJECT_ID, person_id),
                                     query_input=dialogflow.types.QueryInput(text=text_input),
                                     query_params=query_params)
        self.reply = df.query_result.fulfillment_text
