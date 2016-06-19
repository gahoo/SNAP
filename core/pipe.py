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


class WorkflowParameter(object):
    """docstring for WorkflowParameter"""
    def __init__(self, workflow_path, project_path, value_file=None):
        super(WorkflowParameter, self).__init__()
        self.workflow_path = workflow_path
        self.project_path = project_path
        self.workflow_name = os.path.basename(workflow_path)
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
            map(self.write, zip(filenames, contents))

    def render(self, output_path):
        def prepareData():
            data4template = dict(project_path=self.project_path)
            for block_name in self.values.keys():
                if block_name == 'Samples':
                    data4template['Samples'] = self.values['Samples']
                else:
                    data4template.update(self.values[block_name])
            return data4template

        def makeAllSamplesContents(data4template):
            def makeSingleSampleContent(sample):
                data = data4template.copy()
                data.update(sample)
                return self.template.render(data)

            return map(makeSingleSampleContent, self.values['Samples'])

        def makePlanContents(plan_type_app, data4template):
            if len(plan_type_app) > 1:
                raise Exception, "The number of app with plan greater than 1"
            the_plans = {}
            for app_name, parameters in plan_type_app.iteritems():
                for parameter_name, plans in parameters.iteritems():
                    for i, plan in enumerate(plans):
                        if not the_plans.has_key(i):
                            the_plans[i]=dict()
                            the_plans[i][app_name] = dict()
                        the_plans[i][app_name][parameter_name] = plan
            pdb.set_trace()
            print the_plans

            return self.template.render(data)

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
            contents = makePlanContents(plan_type_app, data4template)
        else:
            content = self.template.render(data4template)
            self.save(output_path, content)
