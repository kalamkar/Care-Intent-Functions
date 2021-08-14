import config
import datetime
import jinja2
import numpy as np
import query

from inspect import getmembers, isfunction
from google.cloud import bigquery


class Context(object):
    def __init__(self):
        self.data = {
            'scheduled_run': False,
            'data': {'systolic': None, 'diastolic': None, 'glucose': None, 'medication': None},
            'message': {'time': None, 'sender': None, 'receiver': None, 'tags': [], 'content': None,
                        'dialogflow': {'intent': None, 'action': None, 'reply': None, 'confidence': None,
                                       'params': {}}},
            'sender': {'name': {'first': None, 'last': None}, 'identifiers': [], 'id': {'type': None, 'value': None}},
            'receiver': {'name': {'first': None, 'last': None}, 'identifiers': [], 'id': {'type': None, 'value': None}},
            'action': {'group': None, 'id': None}
        }
        self.env = jinja2.Environment(loader=jinja2.BaseLoader(), trim_blocks=True, lstrip_blocks=True)
        self.env.filters['history'] = self.history
        self.env.filters['np'] = self.numpy

    def numpy(self, value, function):
        functions = {name: value for name, value in getmembers(np, isfunction)}
        if not function or function not in functions:
            return value
        return functions[function](value)

    def history(self, resource, var, duration='1w'):
        if not var or not resource or type(resource) != dict:
            return []
        if 'id' in resource and 'value' in resource['id']:
            source = resource['id']['value']
        elif 'value' in resource:
            source = resource['value']
        else:
            return []
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=query.get_duration_secs(duration))
        start_time = start_time.isoformat()
        bq = bigquery.Client()
        q = 'SELECT number FROM {project}.live.tsdata, UNNEST(data) ' \
            'WHERE source.value = "{source}" AND name = "{name}" AND time > TIMESTAMP("{start}") ORDER BY time'. \
            format(project=config.PROJECT_ID, name=var, start=start_time, source=source)
        print(q)
        return [row['number'] for row in bq.query(q)]

    def evaluate(self, expression):
        try:
            return self.env.from_string(expression).render(self.data) == str(True)
        except:
            return False

    def render(self, content):
        if type(content) == str:
            try:
                return self.env.from_string(content).render(self.data)
            except:
                print('Failed rendering ' + content)
        elif type(content) == list:
            return [self.render(item) for item in content]
        elif type(content) == dict:
            return {self.render(name): self.render(value) for name, value in content.items()}
        return content

    def set(self, name, value):
        if type(value) == dict:
            for param in ['login', 'tokens']:
                if param in value:
                    del value[param]
        merge(self.data, {name: value})

    def clear(self, name):
        if name in self.data:
            del self.data[name]

    def get(self, name):
        tokens = name.split('.') if name else []
        try:
            if len(tokens) == 1:
                return self.data[tokens[0]]
            elif len(tokens) == 2:
                return self.data[tokens[0]][tokens[1]]
            elif len(tokens) == 3:
                return self.data[tokens[0]][tokens[1]][tokens[2]]
            elif len(tokens) == 4:
                return self.data[tokens[0]][tokens[1]][tokens[2]][tokens[3]]
        except KeyError:
            return None
        except TypeError:
            return None
        return None

    def update(self, patch):
        self.data.update(patch)


def merge(destination, source):
    """
    run me with nosetests --with-doctest file.py

    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> merge(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            if key not in destination or not destination[key]:
                destination[key] = {}
            node = destination[key]
            merge(node, value)
        else:
            destination[key] = value
    return destination
