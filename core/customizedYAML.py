import yaml
import os
import re
import pdb
from collections import OrderedDict

# codes form stackoverflow.com
# http://stackoverflow.com/questions/6432605/any-yaml-libraries-in-python-that-support-dumping-of-long-strings-as-block-liter
# http://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data

class folded_unicode(unicode): pass
class literal_unicode(unicode): pass
class quoted(str): pass
class literal(str): pass


def folded_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='>')
def literal_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='|')
def quoted_presenter(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')
def literal_presenter(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
def ordered_dict_presenter(dumper, data):
    return dumper.represent_dict(data.items())


yaml.add_representer(folded_unicode, folded_unicode_representer)
yaml.add_representer(literal_unicode, literal_unicode_representer)
yaml.add_representer(quoted, quoted_presenter)
yaml.add_representer(literal, literal_presenter)
yaml.add_representer(OrderedDict, ordered_dict_presenter)


def include_constructor(loader, node):
    filename = loader.construct_scalar(node)
    if not os.path.exists(filename):
        stream_filename = os.path.abspath(loader.stream.name)
        stream_dirname = os.path.dirname(stream_filename)
        filename = os.path.join(stream_dirname, filename)

    with open(filename) as f:
        data = yaml.load(f)
    return data

yaml.add_constructor(u'!include', include_constructor)


def range_constructor(loader, node):
    data = loader.construct_scalar(node)
    match = range_regex.match(data)
    if match:
        start, stop = map(int, match.groups())
        return range(start, stop + 1)
    else:
        raise ValueError('Invalid range: %s' % data)

range_regex = re.compile(r'^\s*(\d+)\s*\.\.\s*(\d+)\s*$')
yaml.add_constructor(u'!range', range_constructor)
yaml.add_implicit_resolver(u'!range', range_regex)


def refer_constructor(loader, node):
    def getRefer(content, keys):
        content = content.replace('!refer ', '')
        ref = yaml.load(content)
        for k in keys:
            ref = ref.get(k)
        return ref

    data = loader.construct_scalar(node)
    keys = data.split('.')
    offset = loader.stream.tell()
    loader.stream.seek(0)
    content = loader.stream.read()
    loader.stream.seek(offset)
    refer = getRefer(content, keys)
    return refer

yaml.add_constructor(u'!refer', refer_constructor)

def mapping_constructor(loader, node):
    data = loader.construct_scalar(node)
    (local, oss) = data.split(':', 1)
    return {'local': local, 'oss': oss}

yaml.add_constructor(u'!mapping', mapping_constructor)

def plan_constructor(loader, node):
    data = loader.construct_scalar(node)
    if ';' in data:
        return {'elements': data.lstrip('~').split(';')}
    match = plan_regex.match(data)
    if match:
        case, control, method = match.groups()
        return {'control': control, 'case': case, 'method': method}
    else:
        raise ValueError('Invalid Plan format: %s' % data)

plan_regex = re.compile(r'^~(\w+)\|?(\w+)?@?(\w+)?')
yaml.add_constructor(u'!plan', plan_constructor)
yaml.add_implicit_resolver(u'!plan', plan_regex)
