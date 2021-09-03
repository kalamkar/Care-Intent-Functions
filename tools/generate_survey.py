import csv
import json
import sys


def main(filename):
    actions = []
    reader = csv.DictReader(open(filename))
    for row in reader:
        condition = ''
        questions = row['Previous Question'].split('\n')
        answers = row['Previous Answer'].split('\n')
        if questions and questions[0].startswith('eval:'):
            condition = questions[0][5:]
            questions = []
        pairs = []
        for i, question in enumerate(questions):
            if i < len(answers) and answers[i] and not answers[i].startswith('!'):
                pairs.append('(person.session.last_question == "{}" and message.nlp.intent == "generic.{}")'.
                             format(question, answers[i]))
            elif i < len(answers) and answers[i] and answers[i].startswith('!'):
                pairs.append('(person.session.last_question == "{}" and message.nlp.intent != "generic.{}")'.
                             format(question, answers[i][1:]))
            else:
                pairs.append('(person.session.last_question == "{}")'.format(question))
        if not condition and pairs:
            condition = '{{(message.status == "received") and (%s)}}' % ' or '.join(pairs)
        actions.append({
            'id': row['Question'] + '.message',
            'type': 'Message',
            'priority': 10,
            'condition': condition,
            'params': {
                'content': row['Message'],
                'receiver': '$person.id'
            }
        })
        if row['Skip Session Update'] != '1':
            actions.append({
                'id': row['Question'] + '.update',
                'type': 'UpdateResource',
                'priority': 5,
                'condition': condition,
                'params': {
                    'content': '{"session.last_question": "%s"}' % (row['Question'] if row['Exit'] != '1' else ''),
                    'identifier': '$person.id'
                }
            })

    json.dump({'actions': actions}, open(filename + '.json', 'w'), indent=2)


if __name__ == '__main__':
    main(sys.argv[1])
