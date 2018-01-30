import os
import pdb
import functools
from core import models
from core.ali.oss import BUCKET, oss2key, is_object_exists, is_size_differ_and_newer
from core.misc import *
from sqlalchemy import create_engine, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

class DB(object):
    def __init__(self, db_path, pipe_path, apps, parameters, dependencies, overwrite=False):
        super(DB, self).__init__()
        self.db_path = db_path
        self.proj_path = os.path.dirname(db_path)
        self.pipe_path = pipe_path
        self.apps = apps
        self.parameters = parameters
        self.app_parameters = self.trim_parameters(parameters)
        self.dependencies = dependencies
        self.engine = create_engine('sqlite:///{db_path}'.format(db_path=self.db_path))
        self.proj = None
        if overwrite and os.path.exists(self.db_path):
            os.remove(self.db_path)
        if not os.path.exists(self.db_path):
            models.Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def trim_parameters(self, parameters):
        skips = ['Inputs', 'Property', 'Parameters', 'CommonData', 'Samples', 'Outputs', 'Conditions', 'CommonParameters']
        return {k:v for k, v in parameters.iteritems() if k not in skips}

    def format(self):
        self.mkProj()
        self.mkInstance()
        # Full Module
        map(self.mkModule, self.dependencies.keys())
        self.mkDepends()
        self.session.commit()

    def add(self):
        snap_db_list = os.path.expanduser("~/.snap/db.yaml")
        db_list = {}
        if os.path.exists(snap_db_list):
            db_list = loadYaml(snap_db_list)
        if db_list is None:
            db_list = {}
        contract_id = self.parameters['CommonParameters']['ContractID']
        db_list[contract_id] = self.db_path
        dumpYaml(snap_db_list, db_list)

    def mkProj(self):
        commom_parameters = self.parameters['CommonParameters']
        self.proj = models.Project(
            name = commom_parameters['ContractID'],
            description = commom_parameters['project_description'],
            type = commom_parameters.get('BACKEND', models.BCS),
            pipe = self.pipe_path,
            path = commom_parameters.get('WORKSPACE', './'),
            max_job = commom_parameters.get('MAX_JOB', 50),
            mns = commom_parameters.get('MNS') )
        self.session.add(self.proj)
        self.session.commit()

    def mkModule(self, module_name):
        module = models.Module(name = module_name)
        self.session.commit()
        for appname in self.dependencies[module_name].keys():
            self.mkApp(self.apps[appname], module)

    def mkInstance(self):
        instance_list = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'instance.txt')
        instances = []
        with open(instance_list, 'r') as instance_file:
            for line in instance_file:
                (Name, CPU, MEM, DiskType, DiskSize, Price) = line.strip().split('\t')
                instances.append(models.Instance(
                name=Name, cpu=CPU, mem=MEM, price=Price,
                disk_type=DiskType, disk_size=DiskSize) )

        self.session.add_all(instances)
        self.session.commit()

    def chooseInstance(self, app):
        instance_id = getAppConfig(app, ['requirements', 'instance', 'id'])
        (cpu, mem, disk_size, disk_type) = map(functools.partial(getResourceConfig, app=app), ['cpu', 'mem', 'disk', 'disk_type'])

        if instance_id is None:
            instance = self.session.query(models.Instance). \
            filter( models.Instance.cpu >= cpu ). \
            filter( models.Instance.mem >= unifyUnit(mem) ). \
            order_by( models.Instance.price ).first()
        else:
            instance = self.session.query(models.Instance).filter_by(name = instance_id).one()

        if instance is None:
            raise LookupError("No proper instance found!")

        return instance

    def mkApp(self, app, module):
        def mkTask(script):
            def mkMapping(mapping):
                m = models.Mapping(
                    name = mapping['name'],
                    source = mapping['source'],
                    destination = mapping['destination'],
                    is_write = mapping['is_write'],
                    is_immediate = mapping['is_immediate'])
                try:
                    self.session.add(m)
                    self.session.commit()
                except IntegrityError:
                    self.session.rollback()
                    m = self.session.query(models.Mapping).filter_by(
                        name = mapping['name'],
                        source = mapping['source'],
                        destination = mapping['destination'],
                        is_write = mapping['is_write'],
                        is_immediate = mapping['is_immediate']).one()
                return m

            script['task'] = models.Task(
                    shell = script['filename'],
                    cpu = cpu,
                    mem = unifyUnit(mem),
                    docker_image = app.docker_image,
                    disk_size = unifyUnit(disk_size),
                    disk_type = disk_type,
                    project = self.proj,
                    module = module,
                    app = app,
                    mapping = map(mkMapping, script['mappings']),
                    instance = instance)
            try:
                self.session.add(script['task'])
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                print dyeWARNING("'{sh}' not unique".format(sh=script['filename']))

        mem = getAppConfig(app, ['requirements', 'resources', 'mem'])
        (cpu, mem, disk_size, disk_type) = map(functools.partial(getResourceConfig, app=app), ['cpu', 'mem', 'disk', 'disk_type'])
        instance = self.chooseInstance(app)
        scripts = [s for s in app.scripts if s['module'] == module.name]

        app = models.App(
            name = app.appname,
            alias = getAppConfig(app, ['name']),
            docker_image = getAppConfig(app, ['requirements', 'container', 'image']),
            instance_image = getAppConfig(app, ['requirements', 'instance', 'image']),
            yaml = app.config_file,
            cpu = cpu,
            mem = unifyUnit(mem),
            disk_size = unifyUnit(disk_size),
            disk_type = disk_type,
            module = module,
            instance = instance)
        self.session.add(app)
        self.session.commit()

        map(mkTask, scripts)

    def mkDepends(self):
        def mkCombTaskDepends(tasks, dep_tasks):
            for task in tasks:
                for dep_task in dep_tasks:
                    task.depend_on.append(dep_task)
            self.session.commit()

        def mkSampleTaskDepends(app, module, dep_app, dep_module):
            def mkEachSampleTaskDepends(sample_name):
                tasks = getSampleTask(app, module, sample_name)
                dep_tasks = getSampleTask(dep_app, dep_module, sample_name)
                mkCombTaskDepends(tasks, dep_tasks)

            map(mkEachSampleTaskDepends, [sample['sample_name'] for sample in self.parameters['Samples']])

        def mkAppDepends(app, module_name, depends):
            for dep_appname in depends[app.name]['depends']:
                if dep_appname in depends:
                    dep_module_name = module_name
                    dep_module = self.session.query(models.Module).filter_by(name = dep_module_name).one()
                    dep_app = self.session.query(models.App).filter_by(name = dep_appname).filter_by(module_id = dep_module.id).one()
                else:
                    dep_module_name = getDepModule(dep_appname)
                    dep_module = self.session.query(models.Module).filter_by(name = dep_module_name).one()
                    dep_app = self.session.query(models.App).filter_by(name = dep_appname).filter_by(module_id = dep_module.id).one()

                if hasSampleName(module_name, app.name) and hasSampleName(dep_module_name, dep_app.name):
                    mkSampleTaskDepends(app, module_name, dep_app, dep_module_name)
                else:
                    tasks = getModuleAppTask(app, module_name)
                    dep_tasks = getModuleAppTask(dep_app, dep_module_name)
                    mkCombTaskDepends(tasks, dep_tasks)

        def getDepModule(dep_appname):
            dep_modules = [k for k, v in self.dependencies.iteritems() if dep_appname in v]
            if len(dep_modules) == 0:
                msg = '{dep_appname} not in any module'.format(dep_appname=dep_appname)
                print dyeFAIL(msg)
                raise KeyError(msg)
            elif len(dep_modules) > 1:
                msg = '{dep_appname} has more than one module: {modules}'.format(dep_appname=dep_appname, modules=dep_modules)
                print dyeFAIL(msg)
                raise KeyError(msg)
            elif len(dep_modules) == 1:
                dep_module = dep_modules[0]
            return dep_module

        def hasSampleName(module, appname):
            return self.dependencies[module][appname]['sh_file'].count('sample_name}}') > 0

        def getModuleAppTask(app, module):
            return [t for t in app.task if t.module.name == module]

        def getSampleTask(app, module, sample_name):
            return [s['task'] for s in self.apps[app.name].scripts if s['task'].module.name == module and s['extra']['sample_name'] == sample_name]

        def mkModuleDepend(name, depends):
            module = self.session.query(models.Module).filter_by(name = name).one()
            for app in module.app:
                mkAppDepends(app, module.name, depends)

        for name, depends in self.dependencies.iteritems():
            mkModuleDepend(name, depends)

    def mkOSSuploadSH(self):
        def addSource(source, destination):
            if source in file_size:
                return
            if not is_object_exists(destination):
                file_size[source] = os.path.getsize(source)
                cmd.append("ossutil cp %s %s" % (source, destination))
            elif is_size_differ_and_newer(source, destination):
                cmd.append("ossutil cp %s %s" % (source, destination))

        def tryAddSourceWithPrefix(source, destination):
            for each_source in glob.glob(source+'*'):
                each_destination = os.path.join(os.path.dirname(destination), os.path.basename(each_source))
                addSource(each_source, each_destination)

        def mkDataUpload():
            for m in self.session.query(models.Mapping). \
                    filter_by(is_write = 0, is_immediate = 0). \
                    filter(models.Mapping.name != 'sh').all():
               if os.path.exists(m.source):
                   addSource(m.source, m.destination)
               else:
                   msg = "{name}:{source} not exist.".format(name = m.name, source = m.source)
                   print dyeFAIL(msg)
                   tryAddSourceWithPrefix(m.source, m.destination)

            content = "\n".join(['set -ex'] + list(set(cmd)))
            print "uploadData2OSS.sh: %d files(%d GB) to upload" % (len(file_size), sum(file_size.values())/2**30)
            script_file = os.path.join(self.proj_path, 'uploadData2OSS.sh')
            write(script_file, content)

        def mkScriptUpload():
            for m in self.session.query(models.Mapping).filter_by(name = 'sh').all():
                addSource(m.source, m.destination)

            content = "\n".join(['set -ex'] + list(set(cmd)))
            print "uploadScripts2OSS.sh: %d files to upload" % len(cmd)
            script_file = os.path.join(self.proj_path, 'uploadScript2OSS.sh')
            write(script_file, content)

        cmd = []
        file_size = {}
        mkDataUpload()
        cmd = []
        mkScriptUpload()

def unifyUnit(size):
    if isinstance(size, int) or isinstance(size, float):
        return float(size)
    elif size.upper().endswith("M"):
        return float(size.upper().strip('M')) / 1024
    elif size.upper().endswith("G"):
        return float(size.upper().strip('G'))
    else:
        raise ValueError("Unkown Unit: {size}".format(size=size))

def getConfig(appconfig, keys):
    for key in keys:
        appconfig = appconfig.get(key)
        if appconfig is None:
            break
    return appconfig

def getAppConfig(app, keys):
    appconfig = app.config['app']
    return getConfig(appconfig, keys)

def getResourceConfig(key, app):
    keys = ['requirements', 'resources']
    keys.append(key)
    return getAppConfig(app, keys)
