import csv
import json
import sys


def csv2actions(csv_dict_reader):
    actions = []
    for row in csv_dict_reader:
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
            condition = '{{from_member and (%s)}}' % ' or '.join(pairs)
        special = row['Special'].split('\n')
        if 'nomessage' not in special:
            actions.append({
                'id': row['Question'] + '.message',
                'type': 'Message',
                'priority': 10,
                'condition': condition,
                'min_action_priority': 9,
                'params': {
                    'content': row['Message'],
                    'receiver': '$person.id'
                }
            })
        if 'ticket' in special:
            actions.append({
                'id': row['Question'],
                'type': 'OpenTicket',
                'priority': 10,
                'condition': condition,
                'params': {
                    'content': row['Message'],
                    'category': row['Tag'],
                    'person_id': '$person.id'
                }
            })
        if 'profile' in special:
            actions.append({
                'id': row['Question'] + '.profile',
                'type': 'UpdateResource',
                'priority': 10,
                'condition': condition,
                'params': {
                    'content': row['Tag'],
                    'list_name': 'tags',
                    'identifier': '$person.id'
                }
            })
        if 'start' in special:
          actions.append({
              'id': row['Question'] + '.update',
              'type': 'UpdateResource',
              'priority': 9,
              'condition': condition,
              'params': {
                  'content':
                      '{"session": {"start": "{{message.time}}", "id":"%s", "lead": "bot", "last_question": "%s"}}'
                      % (row['Question'], row['Question']),
                  'identifier': '$person.id'
              }
          })
        elif 'noupdate' not in special or 'end' in special:
            actions.append({
                'id': row['Question'] + '.update',
                'type': 'UpdateResource',
                'priority': 9,
                'condition': condition,
                'params': {
                    'content': '{"session.last_question": "%s"}' % (row['Question'] if 'end' not in special else ''),
                    'identifier': '$person.id'
                }
            })

    actions.append({
        'id': 'survey.answer.record',
        'type': 'UpdateData',
        'priority': 9,
        'condition': '{{from_member and person.session.last_question != ""}}',
        'params': {
            'content': '{"{{person.session.last_question}}": "{{message.content}}"}',
            'tags': 'survey',
            'source_id': '$person.id'
        }
    })
    return actions


if __name__ == '__main__':
    json.dump({'actions': csv2actions(csv.DictReader(open(sys.argv[1])))}, open(sys.argv[1] + '.json', 'w'), indent=2)
