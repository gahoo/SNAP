import yaml
import os
import functools
import md5
import copy
import pdb
import sys
import errno
from jinja2 import Template
from yamlRepresenter import folded_unicode, literal_unicode
from app import App


class WorkflowParameter(object):
    """docstring for WorkflowParameter"""
    def __init__(self, workflow_path, project_path, value_file=None):
        super(WorkflowParameter, self).__init__()
        self.workflow_path = os.path.abspath(workflow_path)
        self.project_path = project_path
        self.workflow_name = os.path.basename(self.workflow_path)
        self.default_file = os.path.join(workflow_path, 'default.yaml')
        self.values = self.loadValues(value_file)
        self.template_file = os.path.join(workflow_path, 'template.yaml')
        self.template = self.loadTemplate()

    def loadYaml(self, filename):
        with open(filename, 'r') as yaml_file:
            return yaml.load(yaml_file)

    def loadValues(self, filename):
        if filename:
            return self.loadYaml(filename)
        else:
            return self.loadYaml(self.default_file)

    def loadTemplate(self):
        with open(self.template_file, 'r') as template_file:
            self.template_content = template_file.read()
            return Template(self.template_content)

    def write(self, filename, content):
        with open(filename, 'w') as output_file:
            output_file.write(content)

    def writeMap(self, name_content):
        (filename, content) = name_content
        self.write(filename, content)

    def save(self, filename, content):
        if filename is None:
            print content
        elif isinstance(filename, file):
            filename.write(content)
        else:
            self.write(filename, content)

    def saveSamples(self, contents, prefix=None):
        if prefix is None:
            print "\n----------\n".join(contents)
        else:
            filenames = ["%s/%s_%s_parameters.yaml" % (prefix, sample['sample_name'], self.workflow_name) for sample in self.values['Samples']]
            map(self.writeMap, zip(filenames, contents))

    def savePlans(self, plan_names, contents, prefix=None):
        if prefix is None:
            print "\n----------\n".join(contents)
        else:
            filenames = ["%s/%s_parameters.yaml" % (prefix, plan_name) for plan_name in plan_names]
            map(self.writeMap, zip(filenames, contents))

    def render(self, output_path):
        def prepareData():
            data4template = dict(project_path=self.project_path)
            for block_name in ['Samples', 'CommonData', 'CommonParameters', self.workflow_name]:
            # for block_name in self.values.keys():
                if block_name == 'Samples':
                    data4template['Samples'] = self.values['Samples']
                else:
                    if block_name in self.values.keys():
                        data4template.update(self.values[block_name])
            return data4template

        def makeAllSamplesContents(data4template):
            def makeSingleSampleContent(sample):
                data = data4template.copy()
                data.update(sample)
                return self.template.render(data)

            return map(makeSingleSampleContent, self.values['Samples'])

        def makePlanContents(plan_type_app, data4template):
            def plan2Name(parameters_dict):
                return "__".join(["%s.%s" % (k,v) for k,v in parameters_dict.items()]).replace(':', "@").replace('&', '-VS-')

            def makeSinglePlanName(app_dict):
                return plan2Name(app_dict.values()[0])

            def makeSinglePlanContent(app_dict):
                data = data4template.copy()
                data.update(app_dict)
                return self.template.render(data)

            def planDict2List(plan_type_app):
                if len(plan_type_app) > 1:
                    pdb.set_trace()
                    raise Exception, "The number of app with plan greater than 1"
                plan_dict = {}
                for app_name, parameters in plan_type_app.iteritems():
                    for parameter_name, plans in parameters.iteritems():
                        for i, plan in enumerate(plans):
                            if not plan_dict.has_key(i):
                                plan_dict[i] = dict()
                                plan_dict[i][app_name] = dict()
                            plan_dict[i][app_name][parameter_name] = plan
                return plan_dict.values()

            plans = planDict2List(plan_type_app)
            plan_names = map(makeSinglePlanName, plans)
            contents = map(makeSinglePlanContent, plans)
            return plan_names, contents

        def findPlanType(data4template):
            def getPlanType(parameters):
                plan_parameter = {}
                for (parameter_name, value) in parameters.iteritems():
                    if isinstance(value, list):
                        plan_parameter[parameter_name] = value
                return plan_parameter

            plan_type_app = {}
            for key, value in data4template.iteritems():
                if isinstance(value, dict):
                    # might be APP
                    (app_name, app_parameter) = (key, value)
                    plan_parameter = getPlanType(app_parameter)
                    if plan_parameter:
                        plan_type_app[app_name] = plan_parameter
            return plan_type_app

        data4template = prepareData()
        plan_type_app = findPlanType(data4template)
        if self.template_content.count('{{sample_name}}'):
            contents = makeAllSamplesContents(data4template)
            self.saveSamples(contents, output_path)
        elif plan_type_app:
            (plan_names, contents) = makePlanContents(plan_type_app, data4template)
            self.savePlans(plan_names, contents, output_path)
        else:
            content = self.template.render(data4template)
            if output_path is None:
                filename = None
            else:
                filename = os.path.join(output_path, "%s_parameters.yaml" % self.workflow_name)
            self.save(filename, content)


class Pipe(dict):
    """The Pipe related things."""
    def __init__(self, pipe_path):
        super(Pipe, self).__init__()
        self.pipe_path = pipe_path
        self.proj_path = ''
        self.apps = {}
        self.parameter_file = ''
        self.parameters = {}

    def new(self):
        pass

    def loadAllApps(self):
        def isApp(files):
            return 'config.yaml' in files

        def loadAPP(app_path):
            app = App(app_path)
            try:
                app.load()
            # except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            except Exception as e:
                print app_path
                print e
            app.shell_path = os.path.basename(os.path.dirname(app_path))
            return app

        excludes = ['example', 'database', '.git']
        for root, dirs, files in os.walk(self.pipe_path, topdown=True, followlinks=True):
            dirs[:] = [d for d in dirs if d not in excludes]
            if isApp(files):
                app = loadAPP(root)
                if app.appname:
                    self.apps[app.appname] = app
                    dirs[:] = []
                    continue

    def build(self, parameter_file=None, proj_path=None):
        self.proj_path = os.path.abspath(proj_path)
        self.loadAllApps()
        self.loadParameters(parameter_file)
        for app in self.apps.values():
            sh_file = os.path.join(self.proj_path, 'shell', app.shell_path, app.config['app']['sh_name'])
            print app.appname, sh_file
            parameters = self.parameters.copy()
            app.build(parameters=parameters, output=sh_file)

    def loadYaml(self, filename):
        with open(filename, 'r') as yaml_file:
            return yaml.load(yaml_file)

    def loadParameters(self, parameter_file=None):
        if parameter_file:
            self.parameter_file = parameter_file
            self.parameters = self.loadYaml(parameter_file)
        else:
            raise ValueError("no parameter file to load.")

    def run(self):
        pass
