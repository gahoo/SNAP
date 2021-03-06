import yaml
import os
import functools
import md5
import copy
import pdb
import sys
import errno
import re
import glob
import time
import git
import shutil
from sqlalchemy import create_engine, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from collections import defaultdict
from jinja2 import Template
from customizedYAML import folded_unicode, literal_unicode, include_constructor
from colorMessage import dyeWARNING, dyeFAIL
from core import models
from core.ali.oss import BUCKET
from core.db import DB
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


class GitProgress(git.RemoteProgress):
    def update(self, op_code, cur_count, max_count=None, message=''):
        #print(op_code, cur_count, max_count, cur_count / (max_count or 100.0), message or "NO MESSAGE")
        progress = "\rDownloading {percent} {msg}".format(percent = 100 * cur_count / (max_count or 100.0), msg = message or "")
        sys.stdout.write(progress)
        sys.stdout.flush()

class Pipe(dict):
    """The Pipe related things."""
    def __init__(self, pipe_path):
        super(Pipe, self).__init__()
        self.pipe_path = pipe_path
        self.proj_path = None
        self.proj = None
        self.apps = {}
        self.parameter_file = ''
        self.parameters = {}
        self.dependencies = {}
        self.pymonitor_conf = []
        self.db_path = None
        self.engine = None
        self.session = None

    def new(self):
        pass

    def add(self, url, overwrite=False):
        if os.path.exists(os.path.join(self.pipe_path, '.git')):
            if overwrite:
                shutil.rmtree(self.pipe_path)
            else:
                print dyeWARNING('pipe already exists')
                os._exit(1)

        repo = git.Repo.clone_from(url, self.pipe_path, progress=GitProgress())
        latest = repo.tags.pop()
        print repo.git.checkout(latest.name)
        self.update_submodules(repo)
        print "\nnew pipe added, using %s" % latest.name
        return latest.name

    def update_submodules(self, repo = None):
        def update_each(submodule):
            print 'Updating {submodule}, {sha}'.format(submodule=submodule.name, sha=submodule.hexsha)
            submodule.update()

        if not repo:
            repo = git.Repo(self.pipe_path)
        map(update_each, repo.submodules)

    def switch(self, version):
        repo = git.Repo(self.pipe_path)
        print repo.git.checkout(version)
        #self.update_submodules(repo)
        print repo.git.submodule('update')
        tags = ", ".join([tag.name for tag in repo.tags])
        print "switch to {version}, available versions: {tags}".format(version=version, tags=tags)

    def upgrade(self, refspec='+refs/heads/*:refs/remotes/origin/*'):
        repo = git.Repo(self.pipe_path)
        remote = repo.remote()
        print "Pulling latest version"
        remote.pull(refspec=refspec, progress=GitProgress())
        latest = repo.tags.pop()
        print repo.git.checkout(latest.name)
        print repo.git.submodule('update')
        tags = ", ".join([tag.name for tag in repo.tags])
        print "upgraded to {version}, available versions: {tags}".format(version=latest.name, tags=tags)
        return latest.name

    def deploy(self, destination, version=None):
        def upload(key, filename, consumed, total):
            BUCKET.put_object_from_file(key, filename)
            if total:
                rate = 100 * consumed / total
                bar = '=' * (rate / 2)
                sys.stdout.write("\r{0}% {1}".format(rate, bar))
                sys.stdout.flush()

        if not version:
            repo = git.Repo(self.pipe_path)
            latest = repo.tags.pop()
            version = latest.name
        self.switch(version)

        excludes = ['example', 'database', 'software', '.gitmodules', '.git', 'config.yaml', 'dependencies.yaml', 'README.md', '.appid']
        files2upload = []
        for root, dirs, files in os.walk(self.pipe_path, topdown=True, followlinks=True):
            dirs[:] = [d for d in dirs if d not in excludes]
            files[:] = [f for f in files if f not in excludes]
            if files:
                files2upload.extend(map(lambda x:os.path.join(root,x), files))

        pipe_name = os.path.basename(self.pipe_path)
        pipe_path = self.pipe_path + '/'
        keys = map(lambda x:os.path.join(destination, pipe_name, version, x.replace(pipe_path, '')), files2upload)
        total = len(files2upload)
        map(upload, keys, files2upload, xrange(1, total+1), [total]*total)

    def destroy(self, bucket='', destination='', version=''):
        shutil.rmtree(self.pipe_path)
        if bucket:
            pipe_name = os.path.basename(self.pipe_path)
            to_delete = os.path.join('oss://', bucket, destination, pipe_name, version, '')
            cmdline = 'ossutil rm -r ' + to_delete
            os.system(cmdline)

    def loadPipe(self):
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

        def isDependency(files):
            return 'dependencies.yaml' in files

        def loadDependency(root):
            depend = self.loadYaml(os.path.join(root, 'dependencies.yaml'))
            module = depend.pop('name')
            self.dependencies[module] = depend

        excludes = ['example', 'database', 'software', '.git']
        for root, dirs, files in os.walk(self.pipe_path, topdown=True, followlinks=True):
            dirs[:] = [d for d in dirs if d not in excludes]
            if isApp(files):
                app = loadAPP(root)
                if app.appname:
                    self.apps[app.appname] = app
                    dirs[:] = []
                    continue
                else:
                    raise IOError("%s is not valid app" % root)

            if isDependency(files):
                loadDependency(root)

    def build(self, parameter_file=None, proj_path=None,
              pymonitor_path='monitor', proj_name=None,
              queue='all.q', priority='RD_test',
              overwrite = False, verbose=False):
        if proj_path:
            self.proj_path = os.path.abspath(proj_path)
        self.verbose = verbose
        self.loadParameters(parameter_file)
        self.loadPipe()
        self.buildApps()
        self.buildDB(overwrite)
        self.buildDepends()
        self.makePymonitorSH(pymonitor_path, proj_name, queue, priority)

    def buildDB(self, overwrite):
        db = DB(
            db_path = os.path.join(self.proj_path, 'snap.db'),
            pipe_path = self.pipe_path,
            apps = self.apps,
            parameters = self.parameters,
            dependencies = self.dependencies,
            overwrite = overwrite)
        db.format()
        db.mkOssSyncSH()
        db.add()

    def buildApps(self):
        def buildEachApp(parameters, module, appname):
            if not self.dependencies.has_key(module):
                raise KeyError('dependencies.yaml has no {module}'.format(module=module))
            if not self.dependencies[module].has_key(appname):
                raise KeyError('dependencies.yaml {module} has no {appname}'.format(module=module, appname=appname))

            defaults = self.dependencies[module][appname].get('defaults')
            if defaults:
                if parameters[module][appname]:
                    parameters[module][appname].update(defaults)
                else:
                    parameters[module][appname] = defaults

            app_path = self.dependencies[module][appname].get('APP_PATH')
            if not app_path:
                app_path = os.path.join(module, appname) + '/'
            parameters[module][appname]['APP_PATH'] = app_path

            try:
                sh_file = os.path.join(self.proj_path, self.dependencies[module][appname]['sh_file'])
            except KeyError:
                raise KeyError('dependencies.yaml {module} {appname} has no "sh_file"'.format(module=module, appname=appname))

            self.checkAppAlias(module, appname)
            self.updateResourceConfig(module, appname)
            self.apps[appname].build(parameters=parameters, module=module, output=sh_file, verbose=self.verbose)

        def buildEachModule(module):
            module_param = dict([(k, self.parameters[k]) for k in ('Samples', 'Groups', 'CommonData', 'CommonParameters', module)])
            for appname in self.parameters[module].keys():
                buildEachApp(module_param.copy(), module, appname)

        for k, v in self.parameters.iteritems():
            if v is None:
                raise ValueError('Module "{module}" contains no app!'.format(module=k))
            if k not in ('Samples', 'Groups', 'CommonData', 'CommonParameters'):
                buildEachModule(k)

    def checkAppAlias(self, module, appname):
        if appname not in self.apps:
            source = self.dependencies[module][appname].get('alias')
            if source is None:
                msg = 'Warning: dependencies.yaml: {module} has no {appname} or {appname} has no alias'.format(module=module, appname=appname)
                print dyeFAIL(msg)
            else:
                self.apps[appname] = copy.deepcopy(self.apps[source])
                self.apps[appname].scripts = []
                self.apps[appname].appname = appname

    def updateResourceConfig(self, module, appname):
        cpu = self.dependencies[module][appname].get('cpu')
        mem = self.dependencies[module][appname].get('mem')
        if cpu:
            self.apps[appname].config['app']['requirements']['resources']['cpu'] = cpu
        if mem:
            self.apps[appname].config['app']['requirements']['resources']['mem'] = mem

    def buildDepends(self):
        def getAppScripts(module, appname):
            if appname not in self.apps:
                print dyeFAIL("Warning: dependencies.yaml: make sure {appname} in {module} is an alias App and not in parameter.conf".format(module=module, appname=appname))
                return []
            return [sh['filename'] for sh in self.apps[appname].scripts if sh['module'] == module]

        def getSampleAppScripts(module, appname, sample_name):
            if appname not in self.apps:
                print dyeFAIL("Warning: dependencies.yaml: make sure {appname} in {module} is an alias App and not in parameter.conf".format(module=module, appname=appname))
                return []
            return [sh['filename'] for sh in self.apps[appname].scripts if sh['module'] == module and sh['extra']['sample_name'] == sample_name]

        def makeAppPymonitorConf(module, appname):
            def buildLines(dep_appname):
                def buildOneLine(dep_script, script):
                    return "%s:%s\t%s:%s" % (
                        dep_script,
                        self.apps[dep_appname].config['app']['requirements']['resources']['mem'],
                        script,
                        self.apps[appname].config['app']['requirements']['resources']['mem'])

                def combLines(A_scripts, B_scripts):
                    for script in A_scripts:
                        for dep_script in B_scripts:
                            line = buildOneLine(dep_script, script)
                            self.pymonitor_conf.append(line)

                def makeSampleLines():
                    def buildSampleLines(sample_name):
                        A_scripts  = getSampleAppScripts(module, appname, sample_name)
                        B_scripts  = getSampleAppScripts(dep_module, dep_appname, sample_name)
                        combLines(A_scripts, B_scripts)
                        sample_scripts[appname].extend(A_scripts)
                        sample_scripts[dep_appname].extend(B_scripts)

                    map(buildSampleLines, [sample['sample_name'] for sample in self.parameters['Samples']])

                def getDepModule(dep_appname):
                    if scripts[module].has_key(dep_appname):
                        dep_module = module
                    else:
                        dep_modules = [k for k, v in scripts.iteritems() if dep_appname in v.keys()]
                        if len(dep_modules) == 0:
                            msg = '{module}{appname} dependence {dep_appname} not in any module'.format(module=module, appname=appname, dep_appname=dep_appname)
                            raise KeyError(dyeFAIL(msg))
                        elif len(dep_modules) > 1:
                            msg = '{module}{appname} dependence {dep_appname} has more than one module: {modules}'.format(module=module, appname=appname, dep_appname=dep_appname, modules=dep_modules)
                            raise KeyError(dyeFAIL(msg))
                        elif len(dep_modules) == 1:
                            dep_module = dep_modules[0]
                    return dep_module

                def hasSampleName(module, appname):
                    return self.dependencies[module][appname]['sh_file'].count('sample_name}}') > 0


                if dep_appname not in self.apps:
                    msg = 'Warning: dependencies.yaml: {appname} dependence {dep_appname} not found'.format(appname=appname, dep_appname=dep_appname)
                    print dyeFAIL(msg)
                    return

                dep_module = getDepModule(dep_appname)

                if hasSampleName(module, appname) and hasSampleName(dep_module, dep_appname):
                    makeSampleLines()
                else:
                    combLines(scripts[module][appname], scripts[dep_module][dep_appname])

            if not self.dependencies[module][appname].has_key('depends'):
                raise KeyError("dependencies.yaml: {module}.{appname} 'depends' not found".format(module=module, appname=appname))
            map(buildLines, self.dependencies[module][appname]['depends'])

        def makeModulePymonitorConf(module):
            for appname in self.dependencies[module].keys():
                makeAppPymonitorConf(module, appname)

        scripts = defaultdict(dict)
        sample_scripts = defaultdict(list)
        for module in self.dependencies.keys():
            for appname in self.dependencies[module].keys():
                scripts[module][appname] = getAppScripts(module, appname)
            self.checkScripts(scripts[module])

        map(makeModulePymonitorConf, self.dependencies.keys())
        pymonitor_conf = os.path.join(self.proj_path, 'monitor.conf')
        content = "\n".join(self.pymonitor_conf)
        self.write(pymonitor_conf, content)
        self.checkScripts(sample_scripts)

    def checkScripts(self, scripts):
        def isExist(filename):
            if not os.path.exists(filename):
                print dyeFAIL("FATAL: %s: %s is not exist." % (appname, filename))

        for appname, sh_files in scripts.iteritems():
            map(isExist, set(sh_files))

    def makePymonitorSH(self, pymonitor_path='monitor', proj_name=None, queue='all.q', priority='RD_test'):
        if proj_name is None:
            proj_name = os.path.basename(self.proj_path)
        pymonitor_conf = os.path.join(self.proj_path, 'monitor.conf')
        content = '{pymonitor_path} taskmonitor -q {queue} -P {priority} -p {proj_name} -i {pymonitor_conf}'
        content = content.format(
            pymonitor_path=pymonitor_path, queue=queue, priority=priority,
            proj_name=proj_name, pymonitor_conf=pymonitor_conf)
        script_file = os.path.join(self.proj_path, 'pymonitor.sh')
        self.write(script_file, content)

    def loadYaml(self, filename):
        with open(filename, 'r') as yaml_file:
            return yaml.load(yaml_file)

    def dumpYaml(self, filename, obj):
        with open(filename, 'w') as yaml_file:
            yaml.dump(obj, yaml_file, default_flow_style=False)

    def write(self, filename, content):
        with open(filename, 'w') as output_file:
            output_file.write(content)

    def loadParameters(self, parameter_file=None):
        if parameter_file:
            self.parameter_file = parameter_file
            self.parameters = self.loadYaml(parameter_file)
            if not self.proj_path:
                self.proj_path = self.parameters['CommonParameters']['WORKSPACE']
        else:
            raise ValueError("no parameter file to load.")

    def run(self):
        pass
