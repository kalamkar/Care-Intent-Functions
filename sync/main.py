import logging

import google.cloud.logging as logger
import openai
from google.cloud import firestore
from twilio.twiml.voice_response import Gather, VoiceResponse

import common
import config

logger.handlers.setup_logging(logger.Client().get_default_handler())

engine = 'davinci-instruct-beta-v3'
temperature = 1
tokens = 32
stop = ''
context = '''
Next meeting is Saturday, December, 18 at 3pm at the church 
(the following meeting will be on Saturday, January, 1 at 3pm). In this second session of the program, 
we will learn about self-monitoring our weight, calculating the amount of fat and calories we eat, 
and tracking our progress. There will be around 10 people. 
The nurse checks if the person will attend the session, addresses any concerns for not attending. 
The nurse uses motivational interviewing conversation.
'''
question = 'We want to help you manage your diabetes. ' \
           'We\'ve made a special appointment for you with our diabetes educator.'


def main(request):
    logging.info(request.form)

    sender, receiver = request.form.get('From'), request.form.get('To')

    if receiver in config.PROXY_PHONE_NUMBERS:
        # This is spam, we are not serving any sync calls on proxy number
        logging.warning('Received spam from %s on %s' % (sender, receiver))
        return '<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>'

    db = firestore.Client()
    contact = {'type': 'phone', 'value': sender}
    person_docs = list(db.collection('persons').where('identifiers', 'array_contains', contact).get())
    if len(person_docs) == 0:
        # Create new person since it doesn't exist
        person_id = common.generate_id()
        person = {'identifiers': [contact]}
        db.collection('persons').document(person_id).set(person)
        person['id'] = {'type': 'person', 'value': person_id}
    else:
        if len(person_docs) > 1:
            logging.warning('More than 1 person for ' + sender)
        person = person_docs[0].to_dict()
        person['id'] = {'type': 'person', 'value': person_docs[0].id}

    tags = ['source:twilio', 'zip:' + request.form.get('FromZip', default='0')]
    sender_id = {'type': 'phone', 'value': sender}
    receiver_id = {'type': 'phone', 'value': receiver}

    # ('CallStatus', 'ringing' or 'in-progress'), ('Direction', 'inbound'), ('DialCallStatus', 'completed')
    response = VoiceResponse()
    gather = Gather(input='speech', timeout=3)
    if request.form.get('CallStatus') == 'ringing':
        gather.say(question)
        response.append(gather)
    elif request.form.get('CallStatus') == 'in-progress':
        openai.api_key = config.OPENAI_KEY
        content = '%s\nNurse: %s\nPatient: %s' % (context, question, request.form.get('SpeechResult'))
        logging.info('%d %f %s' % (tokens, temperature, content))
        response = openai.Completion.create(
            engine=engine,
            prompt=content,
            temperature=temperature,
            max_tokens=tokens,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
            stop=stop.split(',')
        )
        reply = response.choices[0].text if response.choices else ''
        gather.say(reply)
        response.append(gather)

    logging.info(str(response))
    return str(response)
