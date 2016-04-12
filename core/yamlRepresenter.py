import yaml
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
