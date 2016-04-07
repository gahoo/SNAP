import yaml
import os

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


class AppFiles(dict):
    """AppFiles"""
    def __init__(self, name, setting):
        super(AppFiles, self).__init__()
        self.update(setting)
        #name of object
        self.enid = name
        self.name = name
        self.path = None
        self.setPath()

    def setPath(self):
        if self.get('name') != None:
            #name form parameter file
            self.path = self.get('name')
        else:
            self.path = "/var/data/%s.%s" % (self.enid, self.__getExt())

    def __getExt(self):
        formats = self.get('formats')
        if isinstance(formats, list):
            return formats[0]
        elif isinstance(formats, str):
            return formats

class App(object):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.type = "genedock"
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
        # self.__loadParameters()

    def __loadParameters(self, parameter_files=None):
        pass

    def setParameters(self):
        def formatParameters(item):
            (name, parameter) = item
            parameter = AppParameter(parameter)
            if self.parameters['parameters'].get(name) != None:
                parameter['value'] = self.parameters['parameters'][name]['value']
            return (name, parameter)


        #
        # def set_file_parameter(config_in_out):
        #     file_parameters = {}
        #     if config_in_out == None:
        #         file_parameters = None
        #     else:
        #         for (file_parameter, settings) in config_in_out.iteritems():
        #             file_parameters[file_parameter] = {'enid': file_parameter,
        #                 'name': "/var/data/%s.%s" % (file_parameter,
        #                     get_ext(settings['formats']))
        #                 }
        #     return file_parameters
        #
        # self.parameters['inputs'] = set_file_parameter(self.config['app']['inputs'])
        # self.parameters['outputs'] = set_file_parameter(self.config['app']['outputs'])
        #
        # for (parameter, settings) in self.config['app']['parameters'].iteritems():
        #     self.parameters['parameters'][parameter] = {'value': settings['default'], 'variable': False}

        # formatParameters(self.config['app']['parameters'], self.parameters['parameters'])
        if self.config['app']['parameters'] != None:
            self.config['app']['parameters'] = dict(map(
                formatParameters,
                self.config['app']['parameters'].iteritems())
                )
            print self.config['app']['parameters']
            print self.config['app']['parameters']['workspace']

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
