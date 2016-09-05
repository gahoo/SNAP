import yaml
import os
import functools
import md5
import copy
import pdb
import sys
import errno
from jinja2 import Template
from customizedYAML import folded_unicode, literal_unicode, include_constructor
from colorMessage import dyeWARNING


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

        def formatValue(value, string="%s%s%s"):
            return string % (self.get('prefix'), self.get('separator'), value)

        def formatFlag():
            if self.get('value') is True:
                string = self.get('prefix')
            else:
                string = ""
            return string

        def formatBoolean():
            return formatValue(self.get('value'))

        def formatString():
            if self.get('quotes') is True:
                string = "%s%s'%s'"
            else:
                string = "%s%s%s"
            return formatValue(self.get('value'), string)

        def formatArray():
            item_setting = self.get('item')
            if item_setting['type'] == 'string' and item_setting['item_quotes'] is True:
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

        if self.get('value') is None:
            self['value'] = self.get('default')

        if self.get('type') == 'flag':
            return formatFlag()
        elif self.get('type') == 'boolean':
            return formatBoolean()
        elif self.get('type') == 'string':
            return formatString()
        elif self.get('type') == 'array':
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
        self.enid = self.get('enid', self.md5_enid(setting))
        self.updatePath()

    def updatePath(self):
        if self.get('name') is not None:
            self.path = self.get('name')
        else:
            self.path = "/var/data/%s.%s" % (self.enid, self.__getExt())

    def __getExt(self):
        formats = self.get('formats')
        if isinstance(formats, list):
            return formats[0]
        elif isinstance(formats, str):
            return formats

    def md5_enid(self, setting):
        enid_md5 = md5.new()
        enid_md5.update(str(setting))
        return enid_md5.hexdigest()


class App(dict):
    """Everything about App"""
    def __init__(self, app_path):
        super(App, self).__init__()
        self.type = ''
        self.appid = ''
        self.appname = ''
        self.module = ''
        self.app_path = app_path
        self.config_file = os.path.join(app_path, 'config.yaml')
        self.parameter_file = None
        self.parameters = {}
        self.isGDParameters = True
        self.scripts = []
        self.shell_path = ''
        self.config = {
            'app': {
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
                    'container': {
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
                'inputs': {
                    'bam': {
                        'hint': "bwa mem bam",
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item': {
                            'separator': " "
                        },
                        'formats': ['bam']
                    }
                },
                'outputs': {
                    'results': {
                        'type': 'file',
                        'required': True,
                        'minitems': 1,
                        'maxitems': 1,
                        'item': {
                            'separator': " "
                        },
                        'formats': ['tgz']
                    }
                },
                'parameters': {
                    'workspace': {
                        'separator': "",
                        'prefix': "",
                        'type': 'string',
                        'required': True,
                        'default': '/data/project/id',
                        'quotes': False,
                        'hint': 'working space'
                    },
                    'is_genedock': {
                        'separator': "",
                        'prefix': "",
                        'type': 'boolean',
                        'required': True,
                        'default': True,
                        'hint': 'on genedock or not'
                    }
                },
                'cmd_template': folded_unicode(
                    u'{% if parameters.is_genedock|string() == "True" %}\n'
                    "mkdir -p {{parameters.workspace}};{{'\\n'}}\n"
                    "ln -s {{inputs.bam[0].path}} {{parameters.workspace}}/samplename.bam;{{'\\n'}}\n"
                    '{% endif %}\n'
                )
            }}

    def load(self):
        if self.app_path == '':
            self.app_path = os.path.dirname(self.config_file)

        self.config = self.loadYaml(self.config_file)
        self.appname = self.config['app']['name']
        appid_file = os.path.join(self.app_path, '.appid')
        if os.path.exists(appid_file):
            self.appid = open(appid_file, 'r').read().strip()

    def loadParameters(self, parameters=None, parameter_file=None):
        def loadParametersFromFile(parameter_file):
            if parameter_file is not None:
                self.parameter_file = parameter_file
                self.parameters = self.loadYaml(parameter_file)
            elif parameter_file is None and self.parameter_file is not None:
                self.parameters = self.loadYaml(self.parameter_file)
            elif parameter_file is None and self.parameter_file is None:
                raise ValueError("no parameter file to load.")

        def checkParametersType():
            self.isGDParameters = 'Inputs' in self.parameters.keys()

        if parameters:
            self.parameters = parameters
        elif parameter_file or self.parameter_file:
            loadParametersFromFile(parameter_file)
        else:
            self.parameters.update({'Inputs': {}, 'Outputs': {}, 'Parameters': {}})

        checkParametersType()

    def setModule(self, module=None):
        def inModule(k, v):
            if not isinstance(v, dict):
                return False
            elif self.appname in v.keys():
                return True
            else:
                return False

        if module:
            self.module = module
        else:
            modules = [k for k, v in self.parameters.iteritems() if inModule(k, v)]
            if len(modules) > 1:
                raise ValueError('modules:%s length > 1' % modules)
            elif len(modules) == 1:
                self.module = modules[0]
            elif len(modules) == 0:
                self.module = None

    def getValue(self, name):
        def getGDvalue(name):
            if self.appname in self.parameters['Parameters']:
                param_dict = self.parameters['Parameters'][self.appname]['parameters'].get(name)
            else:
                param_dict = None

            if param_dict:
                return param_dict['value']
            else:
                return None

        def getGHvalue(name):
            value = self.loadParameterValue(name)

            if not value:
                value = self.parameters['CommonParameters'].get(name)
            return value

        if self.isGDParameters:
            return getGDvalue(name)
        else:
            return getGHvalue(name)

    def loadParameterValue(self, name):
        try:
            value = self.parameters[self.module][self.appname].get(name)
        except (KeyError, AttributeError) as e:
            value = None
        return value

    def getFilePath(self, name):
        def getGDfilePath(name):
            pass

        def getGHfilePath(name):
            # Parameters App
            file_path = self.loadParameterValue(name)
            # Parameters CommonData
            if not file_path:
                file_path = self.parameters['CommonData'].get(name)
            # Parameters defaults
            if not file_path:
                if name in self.config['app']['inputs'].keys():
                    file_path = renderDefaultPath('inputs')
                if (self.config['app']['outputs']) and (name in self.config['app']['outputs'].keys()):
                    file_path = renderDefaultPath('outputs')

            if isinstance(file_path, list):
                return file_path
            else:
                return [file_path]

        def renderDefaultPath(file_type):
            def renderPath(sample):
                extra = {'sample_name': sample['sample_name']}
                return self.renderScript(path_template, extra=extra)

            # notice: this WORKSPACE might cause inconsistance with -out using pipe build
            WORKSPACE = self.parameters['CommonParameters'].get('WORKSPACE')
            path_template = self.config['app'][file_type][name].get('default')
            if path_template is None:
                raise KeyError('%s:%s.%s has no default' % (self.appname, file_type, name))

            if path_template == '':
                msg = 'Warning: %s:%s.%s has empty default' % (self.appname, file_type, name)
                print dyeWARNING(msg)
                return ['']

            if '{{extra.sample_name}}' in path_template:
                return map(renderPath, self.parameters['Samples'])
            else:
                return [path_template]

        if self.isGDParameters:
            return getGDfilePath(name)
        else:
            return getGHfilePath(name)

    def setParameters(self):
        def newParameters(item):
            (name, settings) = item

            value = self.getValue(name)
            if not value:
                value = settings['default']

            new_settings = {
                'hint': settings['hint'],
                'required': settings['required'],
                'type': settings['type'],
                'value': value,
                'variable': True
                }
            return (name, new_settings)

        def newFiles(item, file_type):
            (name, settings) = item

            file_paths = self.getFilePath(name)
            if not file_paths:
                file_paths = ["/path/to/data/to/load/%s" % name]

            _property = {
                'block_file': {
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

            data = []
            for file_path in file_paths:
                data.append({
                    'enid': name,
                    'name': file_path,
                    'property': _property,
                    'description': "%s file" % name
                })

            if file_type == 'Inputs':
                prefix = 'loaddata'
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = data
                new_settings['category'] = prefix
                new_settings['required'] = settings['required']
            elif file_type == 'Outputs':
                prefix = 'storedata'
                new_settings['alias'] = "%s %s" % (prefix, name)
                new_settings['data'] = data
            else:
                raise TypeError("file_type can only be Inputs or Outputs")
            return ("%s_%s" % (prefix, name), new_settings)

        newInputFiles = functools.partial(newFiles, file_type='Inputs')
        newOutputFiles = functools.partial(newFiles, file_type='Outputs')

        def newOthers():
            self.parameters['Conditions'] = {'schedule': ""}
            self.parameters['Property'] = {
                'CDN': {'required': True},
                'reference_task': [{'id': None}],
                'water_mark': {'required': True, 'style': None},
                'description': "test_%s" % self.config['app']['name'],
                'name': "test_%s" % self.config['app']['name']
                }

        def formatParameters(item):
            (name, settings) = item
            parameter = AppParameter(settings)
            parameter['value'] = self.getValue(name)
            return (name, parameter)

        def formatFiles(item, file_type):
            (name, settings) = item
            # settings = settings.copy()
            if file_type == 'Inputs':
                block_name = "loaddata_%s" % name
            elif file_type == 'Outputs':
                block_name = "storedata_%s" % name
            else:
                raise TypeError("file_type shold be Inputs or Outputs")

            files_parameter = self.parameters[file_type].get(block_name)
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

        def makeParameters(name):
            lower_name = name.lower()
            if self.config['app'][lower_name] is not None and needNew:
                new_params = map(newFunc[name], self.config['app'][lower_name].iteritems())
                self.parameters[name] = dict(new_params)
            if self.config['app'][lower_name] is not None:
                params = map(formatFunc[lower_name], self.config['app'][lower_name].iteritems())
                self[lower_name] = dict(params)

        newFunc = {
            'Parameters': newParameters,
            'Inputs': newInputFiles,
            'Outputs': newOutputFiles,
            }

        formatFunc = {
            'parameters': formatParameters,
            'inputs': formatInputFiles,
            'outputs': formatOutputFiles,
            }

        needNew = not self.parameters or not self.isGDParameters
        self.setModule()
        map(makeParameters, ['Parameters', 'Inputs', 'Outputs'])
        if needNew:
            newOthers()

    def new(self):
        createDir = lambda folder : self.mkdir_p("%s/%s" % (self.app_path, folder))
        touchFile = lambda filename: open("%s/%s" % (self.app_path, filename), 'a').close()

        map(createDir, ['bin', 'lib', 'test'])
        map(touchFile, ['Dockerfile', 'README.md'])
        self.dumpYaml(self.config, self.config_file)

    def mkdir_p(self, path):
        try:
            if path:
                os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def check(self):
        """
        check if the config file is good to go.
        """
        pass

    def build(self, parameters=None, parameter_file=None, output=None):
        self.shell_path = output
        self.load()
        self.loadParameters(parameters, parameter_file)
        self.setParameters()
        if self.isGDParameters:
            script = self.renderScript()
            self.write(script, output)
        else:
            self.renderScripts()
            self.writeScripts()

    def renderScript(self, cmd_template=None, parameters=None, extra=None):
        if cmd_template is None:
            cmd_template = self.config['app']['cmd_template']
        if not parameters:
            parameters = self.get('parameters')
        template = Template(cmd_template)
        return template.render(
            inputs = self.get('inputs'),
            outputs = self.get('outputs'),
            parameters = parameters,
            extra = extra,
            )

    def renderScripts(self):
        def renderSamples():
            for sample in self.parameters['Samples']:
                self['parameters']['sample_name']['value'] = sample['sample_name']
                renderEachParam(extra=None)

        def renderListParam(params, list_params_name):
            def seperateParams(params, list_params_name):
                new_params = params.copy()
                list_params = dict()
                for param_name in list_params_name:
                    list_params[param_name] = new_params.pop(param_name)
                return list_params, new_params

            def hasSameLength(list_params):
                return len(set(map(len, list_params.values()))) == 1

            def paramDict2List(list_params, new_params):
                '''
                {A:[1, ...], B:[2, ...]} => [{A:1, B:2}, ...]
                '''
                def updateDict(idx):
                    d = new_params.copy()
                    d.update(getIndDict(idx))
                    return d

                def getIndDict(idx):
                    '''
                    make each {A:1, B:2}
                    '''
                    return dict([(k, list_params[k][idx]) for k in list_params.keys()])

                cnt = len(list_params.values()[0])
                return map(updateDict, range(cnt))

            def setParam(param_name, value):
                self['parameters'][param_name]['value'] = value

            (list_params, new_params) = seperateParams(params, list_params_name)
            if hasSameLength(list_params):
                params_list = paramDict2List(list_params, new_params)
            else:
                raise ValueError('%s has different length' % list_params_name)

            for n, param_dict in enumerate(params_list):
                map(setParam, param_dict.keys(), param_dict.values())
                param_dict['i'] = n
                renderEachParam(extra=param_dict)

        def renderEachParam(template=None, extra=None):
            if self.shell_path:
                script_file = self.renderScript(self.shell_path, extra=extra)
            else:
                script_file = None
            script = self.renderScript(template, extra=extra)
            if script.count('{{parameters'):
                script = self.renderScript(script, extra=extra)
            self.scripts.append({"filename": script_file, "content": script})

        def findListParams(params):
            isList = lambda value: isinstance(value, list)
            param_names = params.keys()
            return [param_names[i] for i, v in enumerate(params.values()) if isList(v)]

        try:
            params = self.parameters[self.module][self.appname]
            list_params_name = findListParams(params)
        except (KeyError, AttributeError) as e:
            # print self.module, self.appname
            params = None
            list_params_name = None

        if 'sample_name' in self.config['app']['parameters'].keys():
            #something wrong around here
            renderSamples()
            self.type = 'sample'
        elif list_params_name:
            renderListParam(params, list_params_name)
            self.type = 'list'
        elif not self.module:
            msg = "Warning: %s not in any module" % self.appname
            print dyeWARNING(msg)
            renderEachParam()
            self.type = 'single'
        else:
            renderEachParam()
            self.type = 'single'

    def writeScripts(self):
        for script in self.scripts:
            self.write(script['content'], script['filename'])

    def write(self, content, filename=None):
        if filename is None:
            sys.stdout.write(content)
            sys.stdout.write('\n-----------------------\n')
        elif filename == '/dev/null':
            #for windows users
            pass
        else:
            self.mkdir_p(os.path.dirname(filename))
            with open(filename, 'w') as f:
                f.write(content)

    def test(self):
        pass

    def run(self):
        pass

    def dump_parameter(self, parameter_file=None):
        if parameter_file is not None:
            self.parameter_file = parameter_file
            self.dumpYaml(self.parameters, parameter_file)

    def loadYaml(self, filename):
        with open(filename, 'r') as yaml_file:
            return yaml.load(yaml_file)

    def dumpYaml(self, obj, filename):
        if filename == None:
            yaml.dump(obj, sys.stdout, default_flow_style=False)
        elif isinstance(filename, file):
            yaml.dump(obj, filename, default_flow_style=False)
        else:
            with open(filename, 'w') as yaml_file:
                yaml.dump(obj, yaml_file, default_flow_style=False)

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
                    'description': {
                        'value': None,
                        'variable': True
                    },
                    'name': {
                        'value': None,
                        'variable': True
                    }
                },
                'type': "system",
                'name': 'storedata'
            }
            return (name, node)

        def addAppNodes():
            buildEnid = lambda name: (name, [{'enid': name}])
            buildParameter = lambda name: (name, {'value': None, 'variable': False})

            node = {
                'node_id': self.config['app']['name'].replace(' ', '_'),
                'alias': self.config['app']['name'],
                'app_id': self.appid,
                'type': "private",
                'name': self.config['app']['name']
            }

            if self.config['app']['inputs']:
                node['inputs'] = dict(map(buildEnid, self.config['app']['inputs'].keys()))
            if self.config['app']['outputs']:
                node['outputs'] = dict(map(buildEnid, self.config['app']['outputs'].keys()))
            if self.config['app']['parameters']:
                node['parameters'] = dict(map(buildParameter, self.config['app']['parameters'].keys()))
            return (self.config['app']['name'], node)

        self.load()
        nodes = None
        if node_type == 'load' and self.config['app']['inputs']:
            nodes = dict(map(addLoadNodes, self.config['app']['inputs'].iteritems()))
        elif node_type == 'store' and self.config['app']['outputs']:
            nodes = dict(map(addStoreNodes, self.config['app']['outputs'].iteritems()))
        elif node_type == 'app':
            nodes = dict([addAppNodes()])
        else:
            pass
            #raise TypeError, "node type error"

        return nodes

    def buildTestWorkflow(self, test_workflow_file=None):
        def makeWorkflow():
            self.load()
            workflow = {
                'name': "test_%s" % self.config['app']['name'],
                'description': "test_%s" % self.config['app']['name'],
                'account': 'lijiaping@genehealth.com',
                'version': 1,
                'nodelist': []
            }

            for node_type in ('load', 'app', 'store'):
                nodes = self.nodes(node_type)
                if nodes:
                    workflow['nodelist'].extend(nodes.values())

            return {'workflow': workflow}

        self.workflow = makeWorkflow()
        self.dumpYaml(self.workflow, test_workflow_file)
