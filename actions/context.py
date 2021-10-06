import common
import config
import datetime
import jinja2
import json
import logging
import numpy as np
import pytz
import re

from inspect import getmembers, isfunction
from google.cloud import bigquery


class Context(object):
    def __init__(self):
        self.data = {'from_member': False, 'to_member': False, 'from_coach': False, 'to_coach': False}
        self.env = jinja2.Environment(loader=jinja2.BaseLoader(), undefined=SilentUndefined)
        self.env.filters['history'] = self.history
        self.env.filters['np'] = self.numpy
        self.env.filters['timediff'] = self.timediff

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
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=common.get_duration_secs(duration))
        start_time = start_time.isoformat()
        bq = bigquery.Client()
        q = 'SELECT number FROM {project}.live.tsdata, UNNEST(data) ' \
            'WHERE source.value = "{source}" AND name = "{name}" AND time > TIMESTAMP("{start}") ' \
            'AND number IS NOT NULL ' \
            'ORDER BY time'.format(project=config.PROJECT_ID, name=var, start=start_time, source=source)
        logging.info(q)
        data = [row['number'] for row in bq.query(q)]
        logging.info(data)
        return data

    def timediff(self, start, end):
        end = end if end else datetime.datetime.utcnow().astimezone(pytz.utc)
        return (end - start).total_seconds()

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
                logging.error('Failed rendering ' + content)
        elif type(content) == list:
            return [self.render(item) for item in content]
        elif type(content) == dict:
            return {self.render(name): self.render(value) for name, value in content.items()}
        return content

    def set(self, name, value):
        merge(self.data, {name: value})

    def clear(self, name):
        if name in self.data:
            del self.data[name]

    def get(self, name):
        tokens = [int(token) if token.isnumeric() else token for token in name.split('.')] if name else []
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

    def get_dict(self, dictionary, exclude_parsing=()):
        params = {}
        for name, value in dictionary.items():
            needs_json_load = False
            variables = re.findall(r'\$[a-z0-9-_.]+', value) if (name not in exclude_parsing) and (
                        type(value) == str) else []
            for var in variables:
                context_value = self.get(var[1:])
                if value == var:
                    value = context_value
                elif type(context_value) == str:
                    value = value.replace(var, context_value)
                elif type(context_value) in [int, float]:
                    value = value.replace(var, str(context_value))
                else:
                    needs_json_load = True
                    try:
                        value = value.replace(var, json.dumps(context_value))
                    except Exception as ex:
                        logging.warning(ex)
            try:
                params[name] = json.loads(value) if needs_json_load else value
            except Exception as ex:
                logging.warning(ex)
                logging.warning(value)
                params[name] = value
        return params


def merge(destination, source):
    """
    run me with nosetests --with-doctest file.py

    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> merge(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        if key in ['login', 'tokens']:
            continue
        if isinstance(value, dict):
            # get node or create one
            if key not in destination or not destination[key]:
                destination[key] = {}
            node = destination[key]
            merge(node, value)
        else:
            destination[key] = value
    return destination


class SilentUndefined(jinja2.Undefined):
    def _fail_with_undefined_error(self, *args, **kwargs):
        logging.debug('%s is undefined' % self._undefined_name)
        return None
