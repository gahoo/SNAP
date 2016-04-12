import yaml
import os
import functools
import random
import string
import copy
import pdb

from yamlRepresenter import folded_unicode, literal_unicode

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

class App(dict):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.type = "genedock"
        self.appid = ''
        self.app_path = app_path
        self.config_file = app_path + '/config.yaml'
        self.parameters = {'Inputs':{}, 'Outputs':{}, 'Parameters':{}}
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
                    },
                    'is_genedock':{
                        'separator': "",
                        'prefix': "",
                        'type': 'boolean',
                        'required': True,
                        'default': True,
                        'hint': 'on genedock or not'
                    }
                },
                'cmd_template': literal_unicode(
                    u'{% if parameters.is_genedock == "True" %}\n'
                    "mkdir -p {{parameters.workspace}};{{'\\n'}}\n"
                    "ln -s {{inputs.bam}} {{parameters.workspace}}/samplename.bam;{{'\\n'}}\n"
                    '{% endif %}\n'
                )
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
            (name, settings) = item

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

            if file_type == 'Inputs':
                prefix = 'loaddata'
                data['enid'] = name
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = [data]
                new_settings['category'] = prefix
                new_settings['required'] = settings['required']
            elif file_type == 'Outputs':
                prefix = 'storedata'
                data['description'] = "%s file" % name
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = [data]
            else:
                raise TypeError, "file_type can only be Inputs or Outputs"
            return ("%s_%s" % (prefix, name), new_settings)

        formatInputFiles = functools.partial(formatFiles, file_type='Inputs')
        formatOutputFiles = functools.partial(formatFiles, file_type='Outputs')

        def mapFormat(item):
            (name, func) = item
            lower_name = name.lower()
            if self.config['app'][lower_name] != None:
                self.parameters[name] = dict(map(func, self.config['app'][lower_name].iteritems()))

        to_format = [
            ('Parameters', formatParameters),
            ('Inputs', formatInputFiles),
            ('Outputs', formatOutputFiles),
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

        self.dumpYaml(self.parameters, parameter_file)

    def loadParameters(self, parameter_file=None):
        with open(parameter_file, 'r') as parameter_fh:
            self.parameters = yaml.load(parameter_fh)

    def setParameters(self):
        def formatParameters(item):
            (name, settings) = item
            parameter = AppParameter(settings)
            if self.parameters['Parameters'].get(name) != None:
                parameter['value'] = self.parameters['Parameters'][name]['value']
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
                files = [AppFile(settings)]
            return (name, files)

        formatInputFiles = functools.partial(formatFiles, file_type='Inputs')
        formatOutputFiles = functools.partial(formatFiles, file_type='Outputs')

        def mapFormat(item):
            (name, func) = item
            if self.config['app'][name] != None:
                self[name] = dict(map(func, self.config['app'][name].iteritems()))

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
        self.dumpYaml(self.config, self.config_file)

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

    def dumpYaml(self, obj, filepath):
        with open(filepath, 'w') as output:
            yaml.dump(obj, output, default_flow_style=False)

    def nodes(self, node_type):
        def addLoadNodes(item):
            (name, settings) = item
            node = {
                'node_id': "loaddata_%s" % name,
                'alias': "load %s" % name,
                'app_id': '55128c58f6f4067d63b956b5',
                'inputs': None,
                'outputs': {'data': {'enid': name}},
                'parameters': None,
                'type': "system",
                'name': 'loaddata'
            }
            return (name, node)

        def addStoreNodes(item):
            (name, settings) = item
            node = {
                'node_id': "storedata_%s" % name,
                'alias': "store %s" % name,
                'app_id': '55128c94f6f4067d63b956b6',
                'inputs': {'data': {'enid': name}},
                'outputs': None,
                'parameters': {
                    'description':{
                        'value': None,
                        'variable': True
                    },
                    'name':{
                        'value': None,
                        'variable': True
                    }
                },
                'type': "system",
                'name': 'storedata'
            }
            return (name, node)

        def addAppNodes():
            buildEnid = lambda name : (name, [{'enid': name}])
            buildParameter = lambda name : (name, {'value':None, 'variable':False})

            node = {
                'node_id': self.config['app']['name'].replace(' ', '_'),
                'alias': self.config['app']['name'],
                'app_id': self.appid,
                'type': "private",
                'name': self.config['app']['name']
            }

            node['inputs']=dict(map(buildEnid, self.config['app']['inputs'].keys()))
            node['outputs']=dict(map(buildEnid, self.config['app']['outputs'].keys()))
            node['parameters']=dict(map(buildParameter, self.config['app']['parameters'].keys()))
            return (self.config['app']['name'], node)

        if node_type == 'load':
            nodes = dict(map(addLoadNodes, self.config['app']['inputs'].iteritems()))
        elif node_type == 'store':
            nodes = dict(map(addStoreNodes, self.config['app']['outputs'].iteritems()))
        elif node_type == 'app':
            nodes = dict([addAppNodes()])
        else:
            raise TypeError, "node type error"

        return nodes

    def buildTestWorkflow(self, test_workflow_file=None):
        def makeWorkflow():
            workflow = {
                'name': "test_%s" % self.config['app']['name'],
                'description': "test_%s" % self.config['app']['name'],
                'account': 'lijiaping@genehealth.com',
                'version': 1,
                'nodelist': []
            }

            for node_type in ('load', 'app', 'store'):
                workflow['nodelist'].extend(self.nodes(node_type).values())

            return {'workflow': workflow}

        def saveTestWorkflow(test_workflow_file):
            if test_workflow_file == None:
                test_workflow_file = '%s/test/test_workflow.yaml' % self.app_path

            self.dumpYaml(self.workflow, test_workflow_file)

        self.workflow = makeWorkflow()
        saveTestWorkflow(test_workflow_file)
