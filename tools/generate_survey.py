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
        priority = 10 if not row['Priority'] else int(row['Priority'])
        special = row['Special'].split('\n')
        if 'nomessage' not in special:
            actions.append({
                'id': row['Question'] + '.message',
                'type': 'Message',
                'priority': priority,
                'condition': condition,
                'min_action_priority': priority - 1,
                'params': {
                    'content': row['Message'],
                    'receiver': '$person.id'
                }
            })
        if 'ticket' in special:
            actions.append({
                'id': row['Question'],
                'type': 'OpenTicket',
                'priority': priority,
                'condition': condition,
                'params': {
                    'content': row['Message'],
                    'category': row['Tag'],
                    'person_id': '$person.id'
                }
            })
        if 'profile' in special:
            actions.append(get_update_action(row['Question'] + '.profile', condition, priority,
                                             content=row['Tag'], list_name='tags'))
        if 'start' in special:
            content = '{"session": {"start": "{{message.time}}", "id":"%s", "lead": "bot", "last_question": "%s"}}'\
                      % (row['Question'], row['Question'])
            actions.append(get_update_action(row['Question'] + '.update', condition, priority - 1, content=content))
        elif 'end' in special:
            actions.append(get_update_action(row['Question'] + '.update', condition, priority - 1,
                                             delete_field='session'))
        elif 'noupdate' not in special:
            actions.append(get_update_action(row['Question'] + '.update', condition, priority - 1,
                                             content='{"session.last_question": "%s"}' % row['Question']))

    actions.append({
        'id': 'survey.answer.record',
        'type': 'UpdateData',
        'priority': 9,
        'condition':
            '{{from_member and person.session.last_question is defined and person.session.last_question != ""}}',
        'params': {
            'content': '{"{{person.session.last_question}}": "{{message.content}}"}',
            'tags': 'survey',
            'source_id': '$person.id'
        }
    })
    return actions


def get_update_action(question_id, condition, priority, **kwargs):
    return {
        'id': question_id,
        'type': 'UpdateResource',
        'priority': priority,
        'condition': condition,
        'params': {
            'identifier': '$person.id'
        } | kwargs
    }


if __name__ == '__main__':
    json.dump({'actions': csv2actions(csv.DictReader(open(sys.argv[1])))}, open(sys.argv[1] + '.json', 'w'), indent=2)
