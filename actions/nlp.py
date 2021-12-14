import logging

import config
import openai
import uuid

from generic import Action
from google.protobuf.json_format import MessageToDict
import dialogflow_v2beta1 as dialogflow


class OpenAI(Action):
    def process(self, engine='davinci-instruct-beta-v3', content=None, reply='reply', temperature=1, tokens=32):
        openai.api_key = config.OPENAI_KEY
        logging.info('%d %d %s' % (tokens, temperature, content))
        response = openai.Completion.create(
            engine=engine,
            prompt=content,
            temperature=temperature,
            max_tokens=tokens,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        self.context_update = {
            'nlp': {
                reply: response.choices[0].text if response.choices else ''
            }
        }


class DialogFlow(Action):
    def process(self, content='', session_id=None, person=None, kb=None, language='en-US'):
        query_params = dialogflow.types.QueryParameters()
        if kb:
            knowledge_base_path = dialogflow.KnowledgeBasesClient.knowledge_base_path(config.PROJECT_ID, kb)
            query_params = dialogflow.types.QueryParameters(knowledge_base_names=[knowledge_base_path])

        if person and 'session' in person and 'context' in person['session']:
            query_params.contexts = [build_df_context(person['id']['value'], name, value)
                                     for name, value in person['session']['context'].items()]
        df_client = dialogflow.SessionsClient()
        if type(content) == list:
            content = ' '.join(content)
        text_input = dialogflow.types.TextInput(text=content[:255], language_code=language)
        if not session_id and person:
            session_id = person['id']['value']
        elif not session_id and not person:
            session_id = str(uuid.uuid4())
        df = df_client.detect_intent(session=df_client.session_path(config.PROJECT_ID, session_id),
                                     query_input=dialogflow.types.QueryInput(text=text_input),
                                     query_params=query_params)
        sentiment_score = df.query_result.sentiment_analysis_result.query_text_sentiment.score

        self.context_update = {
            'nlp': {
                'intent': df.query_result.intent.display_name,
                'action': df.query_result.action,
                'sentiment_score': sentiment_score,
                'reply': df.query_result.fulfillment_text,
                'confidence': int(df.query_result.intent_detection_confidence * 100),
                'params': MessageToDict(df.query_result.parameters.items())
            }
        }


def build_df_context(session_id, name, data):
    df_context = dialogflow.types.Context(name='projects/{project}/agent/sessions/{session}/contexts/{name}'.format(
        project=config.PROJECT_ID, session=session_id, name=name))
    if 'lifespanCount' in data:
        df_context.lifespan_count = data['lifespanCount']
    if 'parameters' in data:
        df_context.parameters.update(data['parameters'])
    return df_context


def get_context_dict(contexts):
    context = {}
    for ctx in contexts:
        data = MessageToDict(ctx)
        name = data['name'].split('/')[-1]
        if name.startswith('__'):
            continue
        context[name] = data
        del data['name']
    return context
