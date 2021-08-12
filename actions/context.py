import config
import datetime
import jinja2
import numpy as np
import query

from inspect import getmembers, isfunction
from google.cloud import bigquery


class Context(object):
    def __init__(self):
        self.data = {}
        self.env = jinja2.Environment(loader=jinja2.BaseLoader(), trim_blocks=True, lstrip_blocks=True)
        self.env.filters['history'] = self.history
        self.env.filters['np'] = self.numpy

    def numpy(self, value, function):
        functions = {name: value for name, value in getmembers(np, isfunction)}
        if not function or function not in functions:
            return value
        return functions[function](value)

    def history(self, var, duration='1w'):
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=query.get_duration_secs(duration))
        start_time = start_time.isoformat()
        bq = bigquery.Client()
        q = 'SELECT number FROM {project}.live.tsdatav1, UNNEST(data) ' \
            'WHERE name = "{name}" AND time > TIMESTAMP("{start}") ORDER BY time'. \
            format(project=config.PROJECT_ID, name=var, start=start_time)
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
        tokens = name.split('.') if name else []
        if len(tokens) == 1:
            self.data[name] = value
        elif len(tokens) == 2:
            self.data[tokens[0]][tokens[1]] = value
        elif len(tokens) == 3:
            self.data[tokens[0]][tokens[1]][tokens[2]] = value
        elif len(tokens) == 4:
            self.data[tokens[0]][tokens[1]][tokens[2]][tokens[3]] = value

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
        return None

    def update(self, patch):
        self.data.update(patch)