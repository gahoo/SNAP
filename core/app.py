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
from colorMessage import dyeWARNING, dyeFAIL
from core.db import DB
from core import models
from core.misc import *
from core.formats import *


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
        self.module = None
        self.app_path = app_path
        self.config_file = os.path.join(app_path, 'config.yaml')
        self.parameter_file = None
        self.dependence_file = None
        self.dependencies = {}
        self.parameters = {}
        self.isGDParameters = True
        self.is_run = False
        self.scripts = []
        self.shell_path = ''
        self.model = None
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

    def loadDefaults(self, dependence_file=None):
        def setDefault(defaults):
            if defaults is None:
                return
            if self.parameters[module][self.appname]:
                self.parameters[module][self.appname].update(defaults)
            else:
                self.parameters[module][self.appname] = defaults

        def setShellPath():
            if self.shell_path is None:
                self.dependence_file = dependence_file
                self.shell_path = depend[self.appname].get('sh_file')

        if dependence_file:
            depend = self.loadYaml(dependence_file)
            module = depend.pop('name')
            self.setModule(module)
            setShellPath()
            defaults = depend[self.appname].get('defaults')
            setDefault(defaults)
            self.dependencies[module] = depend

    def setModule(self, module=None):
        def inModule(k, v):
            if not isinstance(v, dict):
                return False
            elif self.appname in v.keys():
                return True
            else:
                return False

        if self.module and module is None:
            return

        if module:
            self.module = module
        else:
            modules = [k for k, v in self.parameters.iteritems() if inModule(k, v)]
            if self.parameter_file:
                parameter_file = self.parameter_file
            else:
                parameter_file = 'parameters.conf'

            if len(modules) > 1:
                msg = 'Warning: {appname} has more than one module in {parameter_file}: {modules}, `{module}` will be used'.format(appname=self.appname, parameter_file=parameter_file,  modules=modules, module=modules[0])
                print dyeFAIL(msg)
                self.module = modules[0]
            elif len(modules) == 1:
                self.module = modules[0]
            elif len(modules) == 0:
                msg = 'Warning: no sign of %s in %s' % (self.appname, parameter_file)
                print dyeWARNING(msg)
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

    def loadParameterValue(self, name, is_oss=False):
        def addWorkspace4FilePath(name, value):
            if isinstance(value, str) and not (value.startswith('/') or value.startswith('oss://')) and (name in self.config['app']['inputs'].keys() or name in self.config['app']['outputs'].keys()):
                if is_oss:
                    WORKSPACE = self.oss_path('project')
                else:
                    WORKSPACE = self.parameters['CommonParameters'].get('WORKSPACE')
                value = os.path.join(WORKSPACE, value)
            return value

        try:
            value = self.parameters[self.module][self.appname].get(name)
            value = addWorkspace4FilePath(name, value)
        except (KeyError, AttributeError) as e:
            value = None
        return value

    def getFilePath(self, name, file_type, is_oss=False):
        def getGDfilePath(name):
            pass

        def getGHfilePath(name):
            # Parameters App
            file_path = self.loadParameterValue(name, is_oss)
            # Parameters CommonData
            if not file_path:
                file_path = self.parameters['CommonData'].get(name)
            # Parameters defaults
            if (not file_path or '{{extra.sample_name}}' in file_path) and (self.config['app'][file_type]) and (name in self.config['app'][file_type].keys()):
                file_path = renderDefaultPath(file_path, file_type)

            if isinstance(file_path, list):
                return file_path
            else:
                return [file_path]

        def renderDefaultPath(path_template, file_type):
            def renderPath(sample):
                extra = {'sample_name': sample['sample_name']}
                return self.renderScript(path_template, extra=extra)

            # notice: this WORKSPACE might cause inconsistance with -out using pipe build
            WORKSPACE = self.parameters['CommonParameters'].get('WORKSPACE')
            if path_template is None or path_template is '':
                path_template = self.config['app'][file_type][name].get('default')

            if path_template is None or path_template is '':
                if name not in ['fq1', 'fq2', 'lib_path']:
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

            file_paths = self.getFilePath(name, file_type.lower())
            oss_paths = self.getFilePath(name, file_type.lower(), True)
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
            for file_path,oss_path in zip(file_paths, oss_paths):
                data.append({
                    'enid': name,
                    'name': file_path,
                    'oss': oss_path,
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

        def addSampleNameParam():
            self.config['app']['parameters']['sample_name'] = {'quotes': False, 'prefix': '', 'separator': '', 'hint': '', 'default': '', 'required': True, 'type': 'string', 'value': None}

        def hasSampleName():
            return self.shell_path is not None and self.shell_path.count('sample_name}}') > 0

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
        if hasSampleName():
            addSampleNameParam()
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

        def checkInputs(input_name):
            def isExists(app_file):
                path = app_file.path
                if (path is not '') and ('{{' not in path) and (not os.path.exists(path)):
                    print dyeWARNING('%s.%s: %s not found' % (self.appname, input_name, path))

            def showFile(data):
                file_path = self.renderScript(data['name'])
                print dyeWARNING("{input}: {name}".format(input=input_name, name=file_path))

            map(isExists, self['inputs'][input_name])
            if self.debug:
                print "==========inputs==========="
                map(showFile, self['inputs'][input_name])

        def checkParameters(param_name):
            print dyeWARNING("{param}: {value}".format(param=param_name, value=self['parameters'][param_name]))

        def checkOutputs(output_name):
            def showFile(data):
                file_path = self.renderScript(data['name'])
                print dyeWARNING("{output}: {name}".format(output=output_name, name=file_path))
            map(showFile, self['outputs'][output_name])

        map(checkInputs, self.get('inputs', []))
        if self.debug:
            print "==========outputs=========="
            map(checkOutputs, self.get('outputs', []))
            print "========parameters========="
            map(checkParameters, self.get('parameters', []))
            print "==========================="

    def build(self, parameters=None, parameter_file=None, dependence_file=None, module=None, output=None, debug=False, benchmark=False):
        self.shell_path = output
        self.debug = debug
        if not self.appname:
            self.load()
        self.loadParameters(parameters, parameter_file)
        self.loadDefaults(dependence_file)
        self.setModule(module)
        self.setParameters()
        if self.isGDParameters:
            script = self.renderScript()
            self.write(script, output)
        else:
            # add debug mode: add *.sh.variable file telling the value of inputs. outputs. parameters. making debug easier
            self.renderScripts()
            self.writeScripts()

    def renderScript(self, cmd_template=None, parameters=None, extra=None):
        if cmd_template is None:
            cmd_template = self.config['app']['cmd_template']
        if not parameters:
            parameters = self.get('parameters')
        samples = self.parameters.get('Samples')
        template = Template(cmd_template)
        return template.render(
            inputs = self.get('inputs'),
            outputs = self.get('outputs'),
            parameters = parameters,
            samples = samples,
            extra = extra,
            )

    def renderScripts(self):
        def renderSamples(list_params_name):
            def getEmptyNameInputs():
                return set([k for k in self['inputs'].keys() if self['inputs'][k][0]['name'] == ''])

            def needRender(inputs, parameters):
                if len(inputs) > 0 or len(parameters) > 0:
                    return True
                else:
                    return False

            def updateInputs(slots):
                oss_clean_data_path = os.path.join(self.oss_path('clean'), sample_dict['sample_name'])
                for k in slots:
                    file_path = data.get(k)
                    if file_path.startswith('oss://'):
                        self['inputs'][k][0]['name'] = file_path.replace('oss://', '/')
                        self['inputs'][k][0]['oss'] = file_path
                    else:
                        self['inputs'][k][0]['name'] = file_path
                        self['inputs'][k][0]['oss'] = os.path.join(oss_clean_data_path, os.path.basename(file_path))
                    self['inputs'][k][0].updatePath()

            def updateParameters(slots):
                for k in slots:
                    self['parameters'][k]['value'] = data.get(k)

            def renderData(data):
                common_inputs = set(data.keys()) & empty_inputs
                common_parameters = set(data.keys()) & set(self['parameters'].keys())
                if needRender(common_inputs, common_parameters):
                    updateInputs(common_inputs)
                    updateParameters(common_parameters)
                    if(list_params_name):
                        params = self.parameters[self.module][self.appname]
                        renderListParam(params, list_params_name, extra=sample_dict)
                    else:
                        renderEachParam(extra=sample_dict)

            empty_inputs = getEmptyNameInputs()
            for sample in self.parameters['Samples']:
                self['parameters']['sample_name']['value'] = sample['sample_name']
                sample_dict = {}
                sample_dict.update(sample)
                sample_data = sample_dict.pop('data')
                for data in sample_data:
                    renderData(data)

        def renderListParam(params, list_params_name, extra=None):
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
                if param_name in self['parameters']:
                    self['parameters'][param_name]['value'] = value
                elif param_name in self['inputs'] or param_name in self['outputs']:
                    pass
                else:
                    print dyeWARNING('Warning: %s is neither parameters nor inputs outputs' % param_name )

            (list_params, new_params) = seperateParams(params, list_params_name)
            if hasSameLength(list_params):
                params_list = paramDict2List(list_params, new_params)
            else:
                raise ValueError('%s has different length' % list_params_name)

            for n, param_dict in enumerate(params_list):
                map(setParam, param_dict.keys(), param_dict.values())
                param_dict['i'] = n
                if(extra):
                    param_dict.update(extra)
                renderEachParam(extra=param_dict)

        def renderEachParam(template=None, extra=None):
            if (self.shell_path and self.dependence_file is None) or self.is_run:
                script_file = self.renderScript(self.shell_path, extra=extra)
                sh_mapping = addScriptMapping(script_file)
                script_file = sh_mapping['source']
                mappings = [sh_mapping]
            else:
                script_file = None
                mappings = []
            [mappings.extend(getMappings(f, 'inputs', extra)) for f in self['inputs']]
            [mappings.extend(getMappings(f, 'outputs', extra)) for f in self['outputs']]

            self.check()
            script = self.renderScript(template, extra=extra)
            if script.count('{{parameters'):
                script = self.renderScript(script, extra=extra)

            self.scripts.append({"filename": script_file, "content": script, "module": self.module, "extra": extra, "mappings": mappings})

        def addScriptMapping(script_file):
            oss_proj_path = self.oss_path('project')
            oss_script_file = script_file.replace(self.parameters['CommonParameters']['WORKSPACE'], '')
            if oss_script_file.startswith('/'):
                 raise ValueError("Auto generate oss script path failed. Because output path is not identical with WORKSPACE: " + self.parameters['CommonParameters']['WORKSPACE'])
            oss_script_file = os.path.join(oss_proj_path, oss_script_file)
            if not script_file.startswith('/'):
                if self.is_run:
                    script_file = os.path.join(self.parameters['CommonParameters']['WORKSPACE'], script_file)
                else:
                    script_file = os.path.abspath(script_file)

            return {
                'name': 'sh',
                'source': script_file,
                'destination': oss_script_file,
                'is_write': False,
                'is_required': True,
                'is_immediate': False}

        def getMappings(name, file_type, extra):
            if file_type == 'inputs':
                is_write = False
            elif file_type == 'outputs':
                is_write = True

            def checkSource(source):
                new_source = source
                if source.startswith('oss://'):
                    new_source = source.replace('oss://', '/')
                    #raise ValueError("{name}:{source} should not starts with oss://".format(name=name, source=source))
                    msg = "Auto Guess Local Path\t{module}.{app}.{name}: {source} => {new_source}"
                    print dyeWARNING(msg.format(module=self.module, app=self.appname, name=name, source=source, new_source = new_source))
                return new_source

            def checkDestination(destination):
                if not destination:
                    msg = "{app}.{name}: destination is empty"
                    print dyeFAIL(msg.format(app=self.appname, name=name))
                    return ''

                new_destination = destination
                if not destination.startswith('oss://'):
                    if destination.startswith('/data/database'):
                        new_destination = destination.replace('/data/', 'oss://igenecode-bcs/')
                    else:
                        folder = os.path.basename(os.path.dirname(destination))
                        filename = os.path.basename(destination)
                        new_destination = os.path.join(self.oss_path('project'), 'database', folder, filename)
                    msg = "Auto Guess OSS Path\t{module}.{app}.{name}: {destination} => {new_destination}"
                    print dyeWARNING(msg.format(module=self.module, app=self.appname, name=name, destination=destination, new_destination = new_destination))
                return new_destination

            def fix_path(each_file):
                if each_file['name'] != '':
                    each_file['name'] = checkSource(each_file['name'])
                    each_file['oss'] = checkDestination(each_file['oss'])
                    each_file.updatePath()

            map(fix_path, self[file_type][name])

            return [{
                'name': name,
                'source': self.renderScript(f['name'], extra=extra),
                'destination': self.renderScript(f['oss'], extra=extra),
                'is_write': is_write,
                'is_immediate': isImmediate(f['name']) and isImmediate(f['oss']),
                'is_required': f['required']
                } for f in self[file_type][name] if f['name'] != '']

        def isImmediate(path):
            if path in self.parameters['CommonData'].values():
                # in common data
                return False
            if path in self.parameters[self.module][self.appname].values():
                return False
            for sample in self.parameters['Samples']:
                for data in sample['data']:
                    if path in data.values():
                        return False
            return True

        def findListParams(params):
            isList = lambda value: isinstance(value, list)
            inParam = lambda key: key in self['parameters']
            param_names = params.keys()
            return [param_names[i] for i, (k, v) in enumerate(params.iteritems()) if isList(v) and inParam(k)]

        def getCommonListParams():
            return dict([(k, v) for k, v in self.parameters['CommonParameters'].iteritems() if isinstance(v, list)])

        try:
            params = self.parameters[self.module][self.appname]
            params.update(getCommonListParams())
            list_params_name = findListParams(params)
        except (KeyError, AttributeError) as e:
            # print self.module, self.appname
            params = None
            list_params_name = None


        if 'sample_name' in self.config['app']['parameters'].keys():
            #something wrong around here
            renderSamples(list_params_name)
            self.config['app']['parameters'].pop('sample_name')
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
                f.write(content.encode('utf-8'))

    def oss_path(self, type):
        return os.path.join('oss://igenecode-bcs/', type, self.parameters['CommonParameters'].get('ContractID'))

    def test(self):
        pass

    def run(self, cpu=None, mem=None, instance=None, disk_type=None, disk_size=None, docker_image=None, cluster=None, all=False, upload=True, show_json=False, **kwargs):
        def update_app(cpu, mem, disk_type, disk_size, instance, docker_image):
            def update_config(conf, name, value):
                if value:
                    conf[name] = value

            def get_config(conf, name, attr):
                value = conf.get(name)
                if value:
                    return value
                else:
                    conf[name] = {attr: None}
                    return conf[name]

            resources = self.config['app']['requirements']['resources']
            instance_conf = get_config(self.config['app']['requirements'], 'instance', 'id')
            docker_image_conf = get_config(self.config['app']['requirements'], 'container', 'image')
            update_config(resources, 'cpu', cpu)
            update_config(resources, 'mem', mem)
            update_config(resources, 'disk_type', disk_type)
            update_config(resources, 'disk', disk_size)
            update_config(instance_conf, 'id', instance)
            update_config(docker_image_conf, 'image', docker_image)

        def prepare_DB():
            db = DB(':memory:',
                pipe_path = os.path.dirname(os.path.abspath(kwargs['dependence_file'])),
                apps = {self.appname: self},
                parameters = self.parameters.copy(),
                dependencies = self.dependencies)
            db.format()
            db.mkOSSuploadSH()
            return db

        def upload_scripts():
            if upload:
                os.system('sh uploadScript2OSS.sh')

        def prepare_project():
            proj = db.session.query(models.Project).first()
            proj.session = db.session
            proj.logger = new_logger(self.appname)
            if cluster:
                proj.update(cluster = cluster)
            return proj

        def submit_jobs(tasks):
            map(lambda x:x.update(debug_mode=True), tasks)
            map(lambda x:x.start(), tasks)
            bcs = map(lambda x:x.bcs[0], tasks)
            mappings = reduce(concat, map(lambda x:x.mapping, tasks))
            print format_bcs_tbl(bcs, True)
            print format_mapping_tbl(mappings)

        def show_json(tasks):
            if show_json:
                map(lambda x:x.show_json(cache=False), tasks)

        kwargs.pop('name')
        kwargs['parameter_file'] = kwargs.pop('param')
        kwargs['dependence_file'] = kwargs.pop('depend')
        self.is_run = True
        self.build(**kwargs)
        update_app(cpu, mem, disk_type, disk_size, instance, docker_image)
        db = prepare_DB()
        upload_scripts()
        proj = prepare_project()
        if all:
            submit_jobs(proj.task)
            show_json(proj.task)
        else:
            submit_jobs([proj.task[0]])
            show_json([proj.task[0]])

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
            buildParameter = lambda name: (name, {'value': None, 'variable': True})
            if not self.appid:
                sys.stderr.write(dyeWARNING('Warning: no app id'))

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
