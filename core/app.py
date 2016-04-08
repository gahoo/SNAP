import yaml
import os
import functools
import random
import string
import copy

class AppParameter(dict):
    """AppParameter"""
    def __init__(self, setting):
        super(AppParameter, self).__init__()
        self.update(setting)
        self.__check()

    def __check(self):
        """
        check if parameter was correct or not
        """
        types = ['integer', 'number', 'float', 'string', 'flag', 'boolean', 'array']
        def checkType():
            if self.get('type') not in types:
                raise TypeError('type must be one of: %s' % types)

        checkType()

    def __str__(self):
        def formatValue(value, string = "%s%s%s"):
            return string % (self.get('prefix'), self.get('separator'), value)

        def formatFlag():
            if self.get('value') == True:
                string = self.get('prefix')
            else:
                string = ""
            return string

        def formatBoolean():
            return formatValue(self.get('value'))

        def formatString():
            if self.get('quotes') == True:
                string = "%s%s'%s'"
            else:
                string = "%s%s%s"
            return formatValue(self.get('value'), string)

        def formatArray():
            item_setting = self.get('item')
            if item_setting['type'] == 'string' and item_setting['item_quotes'] == True:
                values = ["'%s'" % value for value in self.get('value')]
            else:
                values = self.get('value')

            if item_setting['is_split']:
                param_array = [formatValue(value) for value in values]
                string = item_setting['separator'].join(param_array)
            else:
                values = item_setting['separator'].join(values)
                string = formatValue(values)
            return string

        def formatNumber():
            return formatValue(self.get('value'))

        if self.get('value') == None:
            self['value'] = self.get('default')

        if self.get('type')=='flag':
            return formatFlag()
        elif self.get('type')=='boolean':
            return formatBoolean()
        elif self.get('type')=='string':
            return formatString()
        elif self.get('type')=='array':
            return formatArray()
        elif self.get('type') in ['integer', 'number', 'float']:
            return formatNumber()
        else:
            self.__check()


class AppFile(dict):
    """AppFile"""
    def __init__(self, setting):
        super(AppFile, self).__init__()
        self.update(setting)
        self.enid = self.get('enid', self.randomize_enid())
        self.updatePath()

    def updatePath(self):
        if self.get('name') != None:
            self.path = self.get('name')
        else:
            self.path = "/var/data/%s.%s" % (self.enid, self.__getExt())

    def __getExt(self):
        formats = self.get('formats')
        if isinstance(formats, list):
            return formats[0]
        elif isinstance(formats, str):
            return formats

    def randomize_enid(self, size=32, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

class App(object):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.type = "genedock"
        self.appid = ''
        self.app_path = app_path
        self.config_file = app_path + '/config.yaml'
        self.parameters = {'inputs':{}, 'outputs':{}, 'parameters':{}}
        self.config = {
            'app':{
                'package': "package_name",
                'category': "category of tools",
                'homepage': None,
                'version': 1,
                'name': "app name",
                'alias': "alias name",
                'description': "",
                'tutorial': "",
                'document_author': "",
                'requirements': {
                    'container':{
                        'type': "docker",
                        'image': 'user/image_name'
                    },
                    'resources': {
                        'cpu': 4,
                        'mem': '4096m',
                        'network': False,
                        'port': [],
                        'disk': '10000m'
                    }
                },
                'inputs':{
                    'bam':{
                        'hint': "bwa mem bam",
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item':{
                            'separator': " "
                        },
                        'formats': ['bam']
                    }
                },
                'outputs':{
                    'results': {
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item':{
                            'separator': " "
                        },
                        'formats': ['tgz']
                    }
                },
                'parameters':{
                    'workspace': {
                        'separator': "",
                        'prefix': "",
                        'type': 'string',
                        'required': True,
                        'default': '/data/project/id',
                        'quotes': False,
                        'hint': 'working space'
                    }
                },
                'cmd_template': "mkdir -p {{parameters.workspace}};{{'\\n'}}"
            }}

    def load(self):
        with open(self.config_file, 'r') as config_file:
            self.config = yaml.load(config_file)
        appid_file = self.app_path + '/.appid'
        if os.path.exists(appid_file):
            self.appid = open(appid_file, 'r').read().strip()
        # self.__loadParameters()

    def newParameters(self, parameter_file):
        """
        make parameter template from config after setParameters init
        """
        def formatParameters(item):
            (name, settings) = item
            new_settings = {
                'hint': settings['hint'],
                'required': settings['required'],
                'type': settings['type'],
                'value': settings['default'],
                'variable': True
                }
            return (name, new_settings)

        def formatFiles(item, file_type):
            (name, file_settings) = item
            if isinstance(file_settings, dict):
                #not setParameters yet
                settings = file_settings
            elif isinstance(file_settings, list):
                #already setParameters
                settings = file_settings[0]

            _property = {
                'block_file':{
                    'block_name': None,
                    'is_block': False,
                    'split_format': 'default'
                }
            }

            new_settings = {
                'alias': 'load %s' % name,
                'formats': settings['formats'],
                'maxitems': settings['maxitems'],
                'minitems': settings['minitems'],
                'type': 'file'
            }

            data = {
                'name': "/path/to/data/to/load/%s" % name,
                'property': _property
                }

            if file_type == 'inputs':
                prefix = 'loaddata'
                data['enid'] = name
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = [data]
                new_settings['category'] = prefix
                new_settings['required'] = settings['required']
            elif file_type == 'outputs':
                prefix = 'storedata'
                data['description'] = "%s file" % name
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = [data]
            return ("%s_%s" % (prefix, name), new_settings)

        formatInputFiles = functools.partial(formatFiles, file_type='inputs')
        formatOutputFiles = functools.partial(formatFiles, file_type='outputs')

        def mapFormat(item):
            (name, func) = item
            if self.config['app'][name] != None:
                self.parameters[name] = dict(map(func, self.config['app'][name].iteritems()))

        to_format = [
            ('parameters', formatParameters),
            ('inputs', formatInputFiles),
            ('outputs', formatOutputFiles),
            ]

        map(mapFormat, to_format)
        self.parameters['Conditions'] = {'schedule': ""}
        self.parameters['Property'] = {
            'CDN': {'required':True},
            'reference_task': [{'id':None}],
            'water_mark': {'required':True, 'style':None},
            'description': "test_%s" % self.config['app']['name'],
            'name': "test_%s" % self.config['app']['name']
            }
        with open(parameter_file, 'w') as parameter_fh:
            yaml.dump(self.parameters, parameter_fh,
                      default_flow_style=False)


    def loadParameters(self, parameter_file=None):
        with open(parameter_file, 'r') as parameter_fh:
            self.parameters = yaml.load(parameter_fh)

    def setParameters(self):
        def formatParameters(item):
            (name, settings) = item
            parameter = AppParameter(settings)
            if self.parameters['parameters'].get(name) != None:
                parameter['value'] = self.parameters['parameters'][name]['value']
            return (name, parameter)

        def formatFiles(item, file_type):
            (name, settings) = item
            # settings = settings.copy()
            files_parameter = self.parameters[file_type].get(name)
            if files_parameter:
                files = []
                for data in files_parameter['data']:
                    settings.update(data)
                    files.append(AppFile(settings))
            else:
                #when self.parameters is empty
                if isinstance(settings, dict):
                    files = [AppFile(settings)]
                elif isinstance(settings, list):
                    #already setParameters once
                    files = settings
            return (name, files)

        formatInputFiles = functools.partial(formatFiles, file_type='inputs')
        formatOutputFiles = functools.partial(formatFiles, file_type='outputs')

        def mapFormat(item):
            (name, func) = item
            if self.config['app'][name] != None:
                self.config['app'][name] = dict(map(func, self.config['app'][name].iteritems()))

        to_format = [
            ('parameters', formatParameters),
            ('inputs', formatInputFiles),
            ('outputs', formatOutputFiles),
            ]

        map(mapFormat, to_format)

    def new(self):
        createDir = lambda folder : os.makedirs("%s/%s" % (self.app_path, folder))
        touchFile = lambda filename: open("%s/%s" % (self.app_path, filename), 'a').close()

        map(createDir, ['bin', 'lib', 'test'])
        map(touchFile, ['Dockerfile', 'README.md'])
        with open(self.config_file, 'w') as config_file:
            # config_file.write(self.sample_config)
            yaml.dump(self.config, config_file, default_flow_style=False)

    def check(self):
        """
        check if the config file is good to go.
        """
        pass

    def build(self):
        pass

    def test(self):
        pass

    def run(self):
        pass

    def dump_parameter(self):
        pass

    def insert_node(self):
        pass
