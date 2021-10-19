import argparse
import csv
import json
import logging
import sys

from itertools import repeat

PRIORITY = 12


def csv2actions(prefix, file):
    actions = []
    for row in csv.DictReader(file):
        row_id = row['Id'].strip()
        if not row_id:
            continue
        question_id = prefix + '.' + row_id
        condition = get_condition(prefix, row['Question'], row['Intent'], row['Param'], row['Session'])
        try:
            row['Message'].strip().encode('ascii').decode()
        except:
            logging.error('Unicode char in %s' % row['Message'])
            sys.exit(-1)
        message = row['Message'].strip().replace('\\n', '\n')
        instructions = [s.strip() for s in row['Instruction'].split('\n')]
        if message and row_id != 'ticket':
            actions.append({
                'id': question_id + '.message',
                'type': 'Message',
                'priority': PRIORITY,
                'condition': condition,
                'min_action_priority': PRIORITY - 1,
                'params': {
                    'content': message,
                    'receiver': '$person.id'
                }
            })
        elif row_id == 'ticket':
            actions.append({
                'id': prefix + '.' + row['Question'].strip().split('\n')[0] + '.ticket',
                'type': 'OpenTicket',
                'priority': PRIORITY,
                'condition': condition,
                'params': {
                    'content': message,
                    'category': instructions[0][1:],
                    'priority': int(row['Priority']) if row['Priority'] else 1,
                    'person_id': '$person.id'
                }
            })
        if 'start' in instructions:
            content = '{"session.id":"%s", "session.lead": "bot", "session.question": "%s", "session.norouting": true,'\
                      '"session.tags": ["survey"]}' % (prefix, question_id)
            actions.append(get_update_action(question_id + '.update', condition, PRIORITY - 1, content=content))
        elif 'end' in instructions:
            actions.append(get_update_action(question_id + '.update', condition, PRIORITY - 1,
                                             delete_field='session'))
        elif 'noupdate' not in instructions and row_id != 'ticket' and message:
            actions.append(get_update_action(question_id + '.update', condition, PRIORITY - 1,
                                             content='{"session.question": "%s"}' % question_id))
        session_tags = list(map(lambda t: t[1:], filter(lambda i: i.startswith('#'), instructions)))
        if session_tags and row_id != 'ticket':
            action_id = (question_id + '.tags') if message else (prefix + '.' + row_id)
            actions.append(get_update_action(action_id, condition, PRIORITY - 1, list_name='session.tags',
                                             content='["%s"]' % '","'.join(session_tags)))

    actions.append({
        'id': prefix + '.answer.record',
        'type': 'UpdateData',
        'priority': PRIORITY - 1,
        'condition': '{{from_member and person.session.id == "%s"}}' % prefix,
        'params': {
            'content': '{"{{person.session.question}}": "{{message.content}}"}',
            'tags': 'survey',
            'source_id': '$person.id'
        }
    })
    actions.append({
        'id': prefix + '.answer.update',
        'type': 'UpdateResource',
        'priority': PRIORITY - 1,
        'condition': '{{from_member and person.session.id == "%s"}}' % prefix,
        'params': {
            'content': '{"session.{{person.session.question}}": "{{message.content}}"}',
            'identifier': '$person.id'
        }
    })
    actions.append({
        'id': prefix + '.session.tags.update',
        'type': 'UpdateResource',
        'priority': PRIORITY - 1,
        'condition': '{{from_member and person.session.id == "%s" '
                     'and message.nlp.params != {} and message.nlp.params != None}}'% prefix,
        'params': {
            'identifier': '$person.id',
            'list_name': 'session.tags',
            'content': '[{% for name, value in message.nlp.params.items() %}'
                            '{%if value is iterable and value is not string and value is not mapping %}'
                                '{% for item in value %}"{{item}}",{% endfor %}'
                            '{% endif %}'
                       '{% endfor %}]'
        }
    })
    return actions


def get_condition(prefix, qcell, icell, pcell, tcell):
    questions = [item.strip() for item in qcell.split('\n')]
    intents = [item.strip() for item in icell.split('\n')]
    params = [item.strip() for item in pcell.split('\n')]
    tags = [item.strip().replace('#', '') for item in tcell.split('\n')]
    if questions and questions[0].startswith('{{'):
        return questions[0]

    maxlen = max(len(questions), len(intents), len(params), len(tags))
    orred = []
    for q, i, p, t in zip(questions + list(repeat('', maxlen - len(questions))),
                          intents + list(repeat('', maxlen - len(intents))),
                          params + list(repeat('', maxlen - len(params))),
                          tags + list(repeat('', maxlen - len(tags)))):
        anded = []
        anded.append('person.session.question == "{}"'.format(prefix + '.' + q)) if q else None
        if i and i.startswith('!'):
            anded.append('message.nlp.intent != "{}"'.format(i[1:]))
        elif i:
            anded.append('message.nlp.intent == "{}"'.format(i))
        if p:
            pvalue, pcheck, pname = p.split(' ')
            anded.append('"{}" {} message.nlp.params.{}'.format(pvalue, pcheck.replace('!', 'not '), pname))
        if t and t.startswith('!'):
            anded.append('"{}" not in person.session.tags'.format(t[1:]))
        elif t:
            anded.append('"{}" in person.session.tags'.format(t))
        orred.append('(%s)' % ' and '.join(anded))
    return '{{from_member and (%s)}}' % (' or '.join(orred) if orred else 'False')


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


def main(argv):
    parser = argparse.ArgumentParser(description='Generate actions for a survey conversation.')
    parser.add_argument('--prefix', help='Prefix of question and action ids.', required=True)
    parser.add_argument('--file', help='File to read policy from.', required=True)
    args = parser.parse_args(argv)

    actions = csv2actions(args.prefix, open(args.file))
    if len(actions) != len(set([a['id'] for a in actions])):
        logging.error('Duplicate action ids')
        return
    json.dump({'actions': actions}, open(args.file + '.json', 'w'), indent=2)


if __name__ == '__main__':
    main(sys.argv[1:])
