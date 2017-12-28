from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, ForeignKey, create_engine, Table
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
from state_machine import *
from crontab import CronTab
from batchcompute.resources import (
    JobDescription, TaskDescription, DAG, AutoCluster, Networks,
    GroupDescription, ClusterDescription, Disks, Notification, )
from batchcompute.resources.cluster import Mounts, MountEntry
from batchcompute import ClientError
from core.ali.bcs import CLIENT
from core.ali import ALI_CONF
from core.ali.oss import BUCKET, oss2key, OSSkeys, read_object
from core.formats import *
from colorMessage import dyeWARNING, dyeFAIL, dyeOKGREEN
from collections import Counter
from oss2.exceptions import NoSuchKey
from oss2 import ObjectIterator
from argparse import Namespace
from flask import Flask
from jinja2 import Template
import getpass
import datetime
import os
import json
import pdb

Base = declarative_base()

CREATED = 0
PENDING = 1
WAITING = 2
RUNNING = 3
STOPPED = 4
FAILED = 5
FINISHED = 6
CLEANED = 7

BCS = 0
SGE = 1
K8S = 2

CLOUD = 0
CLOUD_EFFICIENT = 1
CLOUD_SSD = 2

def catchClientError(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except ClientError, e:
            if e.status == 404:
                print dyeFAIL(str(e))
            else:
                raise e
    return wrapper

class Project(Base):
    __tablename__ = 'project'

    id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)
    description = Column(String)
    owner = Column(String, nullable=False, default=getpass.getuser())
    status = Column(Integer, nullable=False, default=CREATED)
    type = Column(Integer, nullable=False, default=BCS)
    pipe = Column(String)
    path = Column(String)
    max_job = Column(Integer, default=50)
    run_cnt = Column(Integer, default=0)
    create_date = Column(DateTime, default=datetime.datetime.now())
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    discount = Column(Float, default=0.1)
    email = Column(String, default="{user}@igenecode.com".format(user=getpass.getuser()))
    mns = Column(String)
    cluster = Column(String)
    task = relationship("Task", back_populates="project")

    session = None
    logger = None

    def __repr__(self):
        return "<Project(id={id}, name={name})>".format(id=self.id, name=self.name)

    def sync(self):
        self.poll()
        if self.reach_max_jobs():
            print dyeWARNING('Reach max job limit')
            os._exit(0)

        to_sync = [t for t in self.task if t.is_waiting or t.is_running]
        to_check = [t for t in self.task if t.is_created or t.is_pending or t.is_failed]
        map(lambda x:x.check(), to_sync)
        map(lambda x:x.check(), to_check)
        self.log_date()

    def log_date(self):
        if not self.start_date:
            self.start_date = datetime.datetime.now()
            self.save()
        all_finished = all([t.is_finished or t.is_cleaned for t in self.task])
        if not self.finish_date and all_finished:
            self.finish_date = datetime.datetime.now()
            self.save()

    def poll(self):
        bcs = self.session.query(Bcs).filter( (Bcs.status=='Waiting') | (Bcs.status=='Running') ).all()
        map(lambda x:x.poll(), bcs)

    def states(self):
        states = [t.aasm_state for t in self.task]
        return Counter(states)

    def progress(self):
        states = self.states()
        total = float(sum(states.values()))
        progress = 100 * (states.get('cleaned', 0) + states.get('finished', 0)) / total
        return round(progress, 2)

    def query_tasks(self, args):
        q = self.session.query(Task)
        if args.id:
            q = q.filter(Task.id.in_(args.id))
        if args.status:
            q = q.filter(Task.aasm_state.in_(args.status))
        if args.shell:
            q = q.filter(Task.shell.like("%" + args.shell + "%"))
        if args.app:
            q = q.join(App).filter(App.name == args.app)
        if args.module:
            q = q.join(Module).filter(Module.name == args.module)
        tasks = q.all()
        return tasks

    def query_mapping_tasks(self, args):
        q = self.session.query(Mapping)
        if args.source:
            q = q.filter(Mapping.source.like("%" + args.source + "%"))
        if args.destination:
            q = q.filter(Mapping.destination.like("%" + args.destination + "%"))
        if args.write is not None:
            q = q.filter(Mapping.is_write == args.write)
        if args.immediate is not None:
            q = q.filter(Mapping.is_immediate == args.immediate)
        mappings = q.all()
        tasks = set(sum([m.task for m in mappings], []))
        return tasks

    def query_bcs(self, args):
        bcs = self.session.query(Bcs).filter_by(id = args.job).one()
        return bcs

    def query_instance(self, args):
        q_filter = {k:v for k, v in args.__dict__.items() if v and k not in ('func', 'mode', 'project')}
        q = self.session.query(Instance)
        if 'name' in q_filter:
            q = q.filter(Instance.name.like("%" + q_filter.pop('name') + "%"))
        q = q.filter_by(**q_filter)
        return q.all()

    def count_active_jobs(self):
        return self.session.query(Bcs).filter(Bcs.status.in_(['Waiting', 'Running'])).count()

    def reach_max_jobs(self):
        return self.count_active_jobs() >= self.max_job

    def cytoscape(self, args):
        cyto = Flask(__name__)
        @cyto.route('/')
        def network():
            template_file = os.path.join(snap_path, 'network.html')
            template = Template(open(template_file).read())

            task_status = app_status = module_status = ''

            if args.mode == 'task':
                task_status = build_status_css()
            if args.mode == 'app':
                app_status = build_status_css()
            if args.mode == 'module':
                module_status = build_status_css()

            sizes = [n['data'].get(args.size, 0) for n in nodes]
            sizes = [s for s in sizes if s != 0]
            if not sizes:
                sizes = [1]

            return template.render(
              edges=json.dumps(edges),
              nodes=json.dumps(nodes),
              layout=args.layout,
              task_status = task_status,
              app_status = app_status,
              module_status = module_status,
              size = args.size,
              min_size = min(sizes),
              max_size = max(sizes),
              progress = self.progress())

        def build_status_css():
            colors = ['#74CBE8', '#f5ff6b', '#E8747C', '#74E883', '#74E883']
            states = ['running', 'stopped', 'failed', 'finished', 'cleaned']
            css = {'pie-size': '80%'}
            for (i, (color, state)) in enumerate(zip(colors, states)):
                css['pie-%s-background-color' % (i + 1)] = color
                css['pie-%s-background-size' % (i + 1)] = 'mapData(%s, 0, 1, 0, 100)' % state
            return ",\n".join(["'{k}': '{v}'".format(k=k, v=v) for k, v in css.items()])

        snap_path = os.path.dirname(os.path.realpath(__file__))
        (edges, nodes) = self.build_network(args)
        cyto.run(host='0.0.0.0', port=args.port)

    def build_network(self, args):
        def build_edges():
            edges = set(map(lambda (x, y): build_each_edge(tasks[x], tasks[y]), depends))
            edges = [(source, target) for source, target in edges if source != target]
            edges = map(lambda (source, target): {'data': {'source': source, 'target': target}}, edges)
            return edges

        def build_each_edge(task, dep_task):
            if args.mode == 'task':
                source = dep_task.id
                target = task.id
            elif args.mode == 'app':
                source = dep_task.app.id
                target = task.app.id
            elif args.mode == 'module':
                source = dep_task.module.id
                target = task.module.id

            return source, target

        def build_nodes(tasks):
            tasks = set(tasks.values())
            apps = set([t.app for t in tasks])
            modules = set([t.module for t in tasks])
            if args.mode == 'task':
                nodes = map(build_task_node, tasks)
                if args.compound in ('app', 'all'):
                    nodes += map(build_app_node, apps)
                if args.compound in ('module', 'all'):
                    nodes += map(build_module_node, modules)
            elif args.mode == 'app':
                nodes = map(build_app_node, apps)
                if args.compound in ('module', 'all'):
                    nodes += map(build_module_node, modules)
            elif args.mode == 'module':
                nodes = map(build_module_node, modules)
            return nodes

        def build_task_node(task):
            if task.bcs:
                bcs = task.bcs[-1]
                elapsed = diff_date(bcs.start_date, bcs.finish_date)
                elapsed = round(elapsed.total_seconds(), 0)
            else:
                elapsed = 0

            if args.compound == 'module':
                parent = "m%s" % task.module.id
            else:
                parent = "m%s.a%s" % (task.module.id, task.app.id)

            if args.size == 'data':
                data = task.size(is_write=True)
            else:
                data = 0

            return {'data': {
              'id': task.id,
              'name': "<{id}> {name}".format(id=task.id, name=os.path.basename(task.shell)),
              task.aasm_state: 1.0,
              'cpu': task.cpu,
              'mem': task.mem,
              'module': task.module.name,
              'app': task.app.name,
              'parent': parent,
              'data': data,
              'elapsed': elapsed},
            'classes': 'task'}

        def build_app_node(app):
            if args.mode == 'app':
               id = app.id
            elif args.mode == 'task':
               id = "m%s.a%s" % (app.module.id, app.id)

            if args.size == 'data':
                data = sum([t.size(is_write=True) for t in app.task])
            else:
                data = 0

            node = {'data': {
              'id': id,
              'name': app.name,
              'module': app.module.name,
              'parent': "m%s" % app.module.id,
              'data': data},
            'classes': 'app'}
            node['data'].update(count_status(app.task))

            if args.size == 'elapsed' and args.mode == 'app':
                node['data']['elapsed'] = total_elapsed(app.task)

            return node

        def build_module_node(module):
            if args.mode == 'module':
               id = module.id
            else:
               id = "m%s" % module.id

            if args.size == 'data':
                data = sum([t.size(is_write=True) for t in module.task])
            else:
                data = 0

            node = {'data': {
              'id': id,
              'name': module.name,
              'data': data},
            'classes': 'module'}
            node['data'].update(count_status(module.task))

            if args.size == 'elapsed' and args.mode == 'module':
                node['data']['elapsed'] = total_elapsed(module.task)

            return node

        def count_status(tasks):
            status = [t.aasm_state for t in tasks]
            status = Counter(status)
            total = float(sum(status.values()))
            status = {k: v/total for k, v in status.items()}

            return status

        def total_elapsed(tasks):
            bcs = [t.bcs[-1] for t in tasks if t.bcs]
            elapsed = 0
            if bcs:
                min_start = min([b.start_date for b in bcs])
                max_finish = max([b.finish_date for b in bcs])
                elapsed = diff_date(min_start, max_finish)
                elapsed = round(elapsed.total_seconds(), 0)
            return int(elapsed)

        def get_depends():
            tasks = self.query_tasks(args)
            tids = set([t.id for t in tasks])
            return self.query_dependence(args, tids)

        def get_tasks():
            dummy_args = Namespace(id = None, status = None, shell = None, app = None, module = None)
            dummy_args.id = set(sum([[s,t] for s,t in depends], []))
            tasks = self.query_tasks(dummy_args)
            return {t.id:t for t in tasks}

        depends = get_depends()
        tasks = get_tasks()
        edges = build_edges()
        nodes = build_nodes(tasks)
        return edges, nodes

    def query_dependence(self, args, tids=None):
        q = self.session.query(dependence_table)
        if tids:
            q = q.filter(dependence_table.c.task_id.in_(tids) | dependence_table.c.depend_task_id.in_(tids))

        return q.all()

    def update(self, **kwargs):
        commom_keys = set(['name', 'description', 'owner', 'status', 'max_job', 'run_cnt', 'discount', 'email', 'mns', 'cluster']) & set(kwargs.keys())
        old_setting = [self.__getattribute__(k) for k in commom_keys]
        [self.__setattr__(k, kwargs[k]) for k in commom_keys]
        kwargs = {k:kwargs[k] for k in commom_keys}
        updated = "\t".join(["(%s %s => %s)" % (k, old, new) for k, old, new in zip(commom_keys, old_setting, kwargs.values())])
        self.save()
        print "Project {id} updated: ".format(id = self.id) + updated

    def save(self):
        try:
            self.session.commit()
        except Exception, e:
            print dyeFAIL(str(e))
            self.session.rollback()

    def clean_files(self, immediate=True):
        def iter_dir(prefix):
            return [obj.key for obj in ObjectIterator(BUCKET, prefix=prefix)]

        if immediate:
            immediate_write = [m.destination for m in self.session.query(Mapping).filter_by(is_write = True, is_immediate = True).all()]
            all_read = [m.destination for m in self.session.query(Mapping).filter_by(is_write = False).all()]
            to_delete = set(all_read) & set(immediate_write)
        else:
            to_delete = set([m.destination for m in self.session.query(Mapping).filter_by(is_write = True).all()])
        dir_to_delete = sum([iter_dir(f) for f in to_delete if f.endswith('/')], [])
        to_delete.update(dir_to_delete)
        oss_keys = OSSkeys(map(oss2key, to_delete))
        for keys in oss_keys:
            result = BUCKET.batch_delete_objects(keys)
        num_keys = len(result.deleted_keys)
        deleted_size = self.size_stat(to_delete)['project']
        print "{num} files({size}G) deleted.".format(num=num_keys, size=deleted_size)

    def clean_bcs(self):
        bcs = self.session.query(Bcs).filter_by(deleted=False).all()
        map(lambda x:x.delete(), bcs)
        map(lambda x:x.update(aasm_state = 'cleaned'), [t for t in self.task if t.is_finished])
        self.session.commit()
        print "{num} jobs deleted.".format(num=len(bcs))

    def size_stat(self, to_delete=None):
        total = 0
        clean_total = 0
        for obj in ObjectIterator(BUCKET, prefix="projects/%s" % self.name):
            if to_delete:
                if obj.key in to_delete:
                    total += obj.size
            else:
                total += obj.size

        for obj in ObjectIterator(BUCKET, prefix="clean/%s" % self.name):
            clean_total += obj.size

        return {'clean': round(float(clean_total) / 2 ** 30, 3), 'project': round(float(total) / 2 ** 30, 3)}

class Module(Base):
    __tablename__ = 'module'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    alias = Column(String)
    yaml = Column(String)

    app = relationship("App", back_populates="module")
    task = relationship("Task", back_populates="module")

    def __repr__(self):
        return "<Module(id={id}, name={name})>".format(id=self.id, name=self.name)

class App(Base):
    __tablename__ = 'app'
    __table_args__ = (UniqueConstraint('name', 'module_id'), )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    alias = Column(String)
    docker_image = Column(String)
    instance_image = Column(String)
    yaml = Column(String)
    cpu = Column(Integer)
    mem = Column(Float)
    disk_size = Column(Float)
    disk_type = Column(String)
    module_id = Column(Integer, ForeignKey('module.id'))
    instance_id = Column(Integer, ForeignKey('instance.id'))

    module = relationship("Module", back_populates="app")
    task = relationship("Task", back_populates="app")
    instance = relationship("Instance", back_populates="app")

    def __repr__(self):
        return "<App(id={id}, name={name})>".format(id=self.id, name=self.name)

dependence_table = Table('dependence', Base.metadata,
    Column('task_id', Integer, ForeignKey('task.id'), primary_key=True),
    Column('depend_task_id', Integer, ForeignKey('task.id'), primary_key=True)
)

task_mapping_table = Table('task_mapping', Base.metadata,
    Column('task_id', Integer, ForeignKey('task.id'), primary_key=True),
    Column('mapping_id', Integer, ForeignKey('mapping.id'), primary_key=True)
)

@acts_as_state_machine
class Task(Base):
    __tablename__ = 'task'
    __table_args__ = (UniqueConstraint('shell', 'project_id'), )
    
    id = Column(Integer, primary_key=True)
    shell = Column(String, nullable=False)
    status = Column(Integer, default=CREATED)
    cpu = Column(Integer)
    mem = Column(Float)
    disk_size = Column(Float)
    disk_type = Column(String)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    module_id = Column(Integer, ForeignKey('module.id'))
    app_id = Column(Integer, ForeignKey('app.id'))
    instance_id = Column(Integer, ForeignKey('instance.id'))

    project = relationship("Project", back_populates="task")
    module = relationship("Module", back_populates="task")
    app = relationship("App", back_populates="task")
    bcs = relationship("Bcs", back_populates="task")
    instance = relationship("Instance", back_populates="task")

    depend_on = relationship("Task", secondary=dependence_table,
        primaryjoin=id==dependence_table.c.task_id,
        secondaryjoin=id==dependence_table.c.depend_task_id,
        backref="depends_on")
    depend_by = relationship("Task", secondary=dependence_table,
        primaryjoin=id==dependence_table.c.depend_task_id,
        secondaryjoin=id==dependence_table.c.task_id,
        backref="depends_by")
    mapping = relationship("Mapping", secondary=task_mapping_table)

    created = State(initial=True)
    pending = State()
    waiting = State()
    running = State()
    stopped = State()
    failed = State()
    finished = State()
    cleaned = State()

    start = Event(from_states=created, to_state=pending)
    restart = Event(from_states=stopped, to_state=waiting)
    stop = Event(from_states=(pending, waiting, running), to_state=stopped)
    submit = Event(from_states=pending, to_state=waiting)
    run = Event(from_states=(waiting, running), to_state=running)
    finish = Event(from_states=(waiting, running), to_state=finished)
    fail = Event(from_states=(waiting, running), to_state=failed)
    retry = Event(from_states=failed, to_state=pending)
    redo = Event(from_states=finished, to_state=pending)
    clean = Event(from_states=(stopped, finished, failed), to_state=cleaned)

    def __repr__(self):
        return "<Task(id={id} sh={shell} status={status})>".format(id=self.id, shell=os.path.basename(self.shell), status=self.status)

    @after('start')
    @after('restart')
    @after('stop')
    @after('submit')
    @after('run')
    @after('finish')
    @after('fail')
    @after('retry')
    @after('redo')
    @after('clean')
    def save(self):
        try:
            self.project.session.commit()
        except Exception, e:
            print dyeFAIL(str(e))
            self.project.session.rollback()

    @after('start')
    @after('redo')
    @after('fail')
    @after('retry')
    def check(self):
        old_state = self.aasm_state
        if self.is_created:
            self.start()
        elif self.is_pending and self.is_dependence_satisfied():
            self.submit()
        elif self.is_waiting or self.is_running:
            self.sync()
        elif self.is_failed and not self.reach_max_failed(3):
            self.retry()

        if self.aasm_state != old_state:
           msg = "{task}\t{module}.{app}\t{sh}\t{old_state} => {state}".format(module=self.module.name, app=self.app.name,
                sh=os.path.basename(self.shell), task=self.id, old_state=old_state, state=self.aasm_state)
           self.project.logger.info(msg)

    def is_dependence_satisfied(self):
        is_finished = [t.is_finished or t.is_cleaned for t in self.depend_on]
        if all(is_finished):
            return True
        else:
            return False

    def reach_max_failed(self, num):
        return len([b for b in self.bcs if b.status == 'Failed']) >= num

    def sync(self):
        bcs = self.bcs[-1]
        if bcs.status == 'Failed':
            self.fail()
        elif bcs.status == 'Waiting':
            pass
        elif bcs.status == 'Running':
            self.run()
        elif bcs.status == 'Finished':
            self.finish()

    @before('submit')
    def new_bcs(self):
        (task_name, task) = self.prepare_task()
        bcs = Bcs(
            name = task_name,
            spot_price_limit = task.AutoCluster.SpotPriceLimit,
            instance = self.instance)
        bcs.dag.add_task(task_name=task_name, task=task)
        bcs.job.Name = "{project}-{sh}".format(project=self.project.name, sh = task_name)
        oss_script_path = [m for m in self.mapping if m.name=='sh'][0].destination
        bcs.job.Description = oss_script_path
        try:
            bcs.submit()
            log_id = "{id}.{name}.0".format(id = bcs.id, name = bcs.name)
            bcs.stdout = os.path.join(task.Parameters.StdoutRedirectPath, "stdout." + log_id)
            bcs.stderr = os.path.join(task.Parameters.StdoutRedirectPath, "stderr." + log_id)
            self.bcs.append(bcs)
        except ClientError, e:
            # better try in check section
            print dyeFAIL(e)
            raise ClientError(e)
            self.fail()

    def prepare_task(self):
        task = TaskDescription()
        (script_name, ext) = os.path.splitext(os.path.basename(self.shell))
        script_name = script_name.replace('.', '_')
        oss_script_path = [m for m in self.mapping if m.name=='sh'][0].destination
        oss_log_path = os.path.join(os.path.dirname(oss_script_path), script_name + '_log') + '/'
        task.Parameters.Command.CommandLine = "sh {sh}".format(sh=self.shell)
        task.Parameters.Command.EnvVars = self.prepare_EnvVars()
        task.Parameters.StdoutRedirectPath = oss_log_path
        task.Parameters.StderrRedirectPath = oss_log_path
        task.WriteSupport = True

        #task.InputMapping = {m.source:m.destination for m in self.mapping if not m.is_write}
        task.OutputMapping = {m.source:m.destination for m in self.mapping if m.is_write}
        #task.LogMapping = {m.source:m.destination for m in self.mapping if m.is_write}
        #task.Mounts.Entries = [MountEntry({'Source': m.destination, 'Destination': m.source, 'WriteSupport':m.is_write}) for m in self.mapping if not m.is_write]
        task.Mounts.Entries = self.prepare_Mounts()

        task.Timeout = 86400 * 3
        task.MaxRetryCount = 0
        if self.project.cluster:
            task.ClusterId = self.project.cluster
        else:
            task.AutoCluster = self.prepare_cluster()
        return script_name, task

    def prepare_Mounts(self):
        def is_rw(m):
            return get_folder(m.Source) not in rw_source and m.Source not in rw_source

        def get_folder(path):
            if not path.endswith('/'):
                path = os.path.dirname(path) + '/'
            return path

        mounts_entries = list(set([self.prepare_MountEntry(m) for m in self.mapping]))
        read_mounts = [m for m in mounts_entries if not m.WriteSupport]
        read_source = set([get_folder(m.Source) for m in read_mounts])

        write_mounts = [m for m in mounts_entries if m.WriteSupport]
        write_source = set([m.Source for m in write_mounts])
        rw_source = read_source & write_source
        if rw_source:
            entries = filter(is_rw, read_mounts)
            write_entries = filter(is_rw, write_mounts)
            entries.extend(write_mounts)
        else:
            entries = read_mounts
        return entries

    def prepare_MountEntry(self, mapping):
        source = mapping.destination
        destination = mapping.source
        if mapping.is_write and not source.endswith('/'):
            source = os.path.dirname(source) + '/'
            destination = os.path.dirname(destination) + '/'
        return MountEntry({'Source': source, 'Destination': destination, 'WriteSupport':mapping.is_write})

    def prepare_EnvVars(self):
        if self.app.docker_image:
            docker_oss_path = os.path.join('oss://', ALI_CONF['bucket'], ALI_CONF['docker_registry_oss_path']) + '/'
            return {"BATCH_COMPUTE_DOCKER_IMAGE": "localhost:5000/" + self.app.docker_image,
                    "BATCH_COMPUTE_DOCKER_REGISTRY_OSS_PATH": docker_oss_path}
        else:
            return {}

    def prepare_cluster(self):
        cluster = AutoCluster()

        if self.app.instance_image is None:
            cluster.ImageId = ALI_CONF['default_image']
        else:
            cluster.ImageId = self.app.instance_image
        cluster.InstanceType = self.instance.name

        if cluster.InstanceType.startswith('bcs.'):
            cluster.ResourceType = "OnDemand"
        else:
            cluster.ResourceType = "Spot"
            #cluster.SpotStrategy = "SpotWithPriceLimit"
            cluster.SpotPriceLimit = self.project.discount * self.instance.price

        cluster.Configs.Disks = self.prepare_disk()
        cluster.Notification = self.prepare_notify()
        cluster.Networks = self.prepare_network()

        return cluster

    def prepare_disk(self):
        def get_common_prefix():
            prefix = os.path.commonprefix([m.source for m in self.mapping if m.is_write])
            if not prefix.endswith('/'):
                prefix = os.path.dirname(prefix) + '/'
            if prefix == '/':
                raise Error("Invalid common prefix: /")
            prefix = "/" + prefix.split('/')[1] + "/"
            return prefix

        def get_disk_type():
            if not self.disk_type:
                if not self.disk_size:
                    return None, None

                if self.disk_size < 40:
                    disk_type = 'system'
                else:
                    disk_type = 'data'
                drive_type = None
            else:
                (disk_type, drive_type) = self.disk_type.split('.')

            return disk_type, drive_type

        def prepare_data_disk():
            if drive_type:
                disks.SystemDisk.Type = drive_type
                disks.DataDisk.Type = drive_type
            disks.SystemDisk.Size = 40
            disks.DataDisk.Size = self.disk_size
            disks.DataDisk.MountPoint = get_common_prefix()

        def prepare_system_disk():
            if drive_type:
                disks.SystemDisk.Type = drive_type
            disks.SystemDisk.Size = 40 if self.disk_size <= 40 else self.disk_size

        disks = Disks()
        (disk_type, drive_type) = get_disk_type()

        if disk_type == 'data':
            prepare_data_disk()
        elif disk_type == 'system':
            prepare_system_disk()
        elif not disk_type and not drive_type:
            pass
        else:
            raise SyntaxError("disk_type '%s' is illegal." % disk_type)

        return disks

    def prepare_notify(self):
        notice = Notification()
        if self.project.mns:
            notice.Topic.Endpoint = self.project.mns
            notice.Topic.Name = self.project.name
            notice.Topic.Events = ['OnJobFailed', 'OnTaskFailed', 'OnInstanceFailed']

        return notice

    def prepare_network(self):
        network = Networks()
        #network.VPC.CidrBlock = "192.168.0.0/16"
        return network

    @before('restart')
    def restart_task(self):
        bcs = self.bcs[-1]
        bcs.restart()
        msg = "{task}\t{module}.{app}\t{sh}\tstopped => pending".format(task=self.id, module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell))
        self.project.logger.info(msg)

    @before('stop')
    def stop_task(self):
        bcs = self.bcs[-1]
        bcs.stop()
        msg = "{task}\t{module}.{app}\t{sh}\t{state} => stopped".format(task=self.id, module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell), state=self.aasm_state)
        self.project.logger.info(msg)

    @before('clean')
    def delete_tasks(self):
        map(lambda x:x.delete(), self.bcs)
        msg = "{task}\t{module}.{app}\t{sh}\t{state} => cleaned".format(task=self.id, module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell), state=self.aasm_state)
        self.project.logger.info(msg)

    @before('clean')
    @before('redo')
    @before('retry')
    def delete_files(self):
        oss_files = [m for m in self.mapping if m.is_immediate and m.is_write]
        map(lambda x:x.oss_delete(), oss_files)

    def show_json(self):
        # json style
        bcs = self.bcs[-1]
        bcs.show_json()

    def show_shell(self):
        sh = [m for m in self.mapping if m.name == 'sh'][0]
        key = oss2key(sh.destination)
        content = read_object(key)
        print dyeOKGREEN("Task Script: " + sh.destination)
        print content

    def show_log(self, type):
        bcs = self.bcs[-1]
        bcs.show_log(type)

    def debug(self, cache=True):
        bcs = self.bcs[-1]
        bcs.debug(cache)

    def show_detail_tbl(self):
        print dyeOKGREEN("Task Details:")
        print format_detail_task(self)

    def show_bcs_tbl(self, with_instance):
        if self.bcs:
            print dyeOKGREEN("Jobs on bcs:")
            print format_bcs_tbl(self.bcs, with_instance).get_string(sortby="create", reversesort=True)

    def show_mapping_tbl(self):
        if self.mapping:
            print dyeOKGREEN("File Mappings:")
            print format_mapping_tbl(self.mapping)

    def show_depends_tbl(self):
        if self.depend_on:
            print dyeOKGREEN("Depends on:")
            print format_tasks_tbl(self.depend_on)
        if self.depend_by:
            print dyeOKGREEN("Depends by:")
            print format_tasks_tbl(self.depend_by)

    def mark_state(self, state):
        self.aasm_state = state
        self.save()

    def update(self, **kwargs):
        if 'instance' in kwargs:
            instance_name = kwargs.pop('instance')
            instance_updated = "\t(instance %s => %s)" % (self.instance.name, instance_name)
            try:
                self.instance = self.project.session.query(Instance).filter_by(name = instance_name).one()
            except NoResultFound, e:
                print dyeFAIL("No such instance: " + instance_name)
                os._exit(1)
        else:
            instance_updated = ""

        commom_keys = set(['cpu', 'mem', 'disk_size', 'disk_type', 'aasm_state']) & set(kwargs.keys())
        old_setting = [self.__getattribute__(k) for k in commom_keys]
        [self.__setattr__(k, kwargs[k]) for k in commom_keys]
        kwargs = {k:kwargs[k] for k in commom_keys}
        updated = "\t".join(["(%s %s => %s)" % (k, old, new) for k, old, new in zip(commom_keys, old_setting, kwargs.values())])
        self.save()
        print "Task {id} updated: ".format(id = self.id) + updated + instance_updated

    @after('redo')
    def update_dependence_chain(self):
        redo_tasks = [t for t in self.depend_by if t.is_finished or t.is_cleaned]
        retry_tasks = [t for t in self.depend_by if t.is_failed]
        map(lambda x:x.redo(), redo_tasks)
        map(lambda x:x.redo(), retry_tasks)

    @after('finish')
    def delete_bcs(self):
        map(lambda x:x.delete(), self.bcs)

    def size(self, is_write=None):
        if is_write is None:
            return sum([m.size() for m in self.mapping])
        else:
            return sum([m.size() for m in self.mapping if m.is_write  == is_write])

class Bcs(Base):
    __tablename__ = 'bcs'

    id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    deleted = Column(Boolean, default=False)
    spot_price = Column(Float)
    stdout = Column(String) # Path
    stderr = Column(String)
    create_date = Column(DateTime, default=datetime.datetime.now())
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    spot_price_limit = Column(Float)
    #cost = Column(Float)
    # could be app_id or module_id even project_id
    task_id = Column(Integer, ForeignKey('task.id'))
    instance_id = Column(Integer, ForeignKey('instance.id'))

    task = relationship("Task", back_populates="bcs")
    instance = relationship("Instance", back_populates="bcs")

    def __repr__(self):
        return "<Bcs(id={id} status={status})>".format(id=self.id, status=self.status)

    def __init__(self, *args, **kwargs):
        self.dag = DAG()
        self.job = JobDescription()
        super(Bcs, self).__init__(*args, **kwargs)

    @catchClientError
    def submit(self):
        self.job.DAG = self.dag
        self.job.Priority = 100
        self.job.AutoRelease = False
        self.id = CLIENT.create_job(self.job).Id
        self.status = 'Waiting'

    @catchClientError
    def poll(self):
        self.state = CLIENT.get_job(self.id)
        self.status = self.state.State
        self.start_date = self.state.StartTime
        self.finish_date = self.state.EndTime
        self.task.project.session.commit()

    @catchClientError
    def show_json(self, cache=True):
        json = self.cache('json')
        if not json or not cache:
            json = CLIENT.get_job_description(self.id)
            self.cache('json', str(json))
        print json

    @catchClientError
    def stop(self):
        CLIENT.stop_job(self.id)
        self.status = 'Stopped'

    @catchClientError
    def restart(self):
        CLIENT.start_job(self.id)
        self.status = 'Waiting'

    @catchClientError
    def delete(self):
        self.delete_log()
        CLIENT.delete_job(self.id)
        self.deleted = True

    def delete_log(self):
        stdout = oss2key(self.stdout)
        stderr = oss2key(self.stderr)
        BUCKET.delete_object(stdout)
        BUCKET.delete_object(stderr)

    def show_log(self, type, cache=True):
        oss_path = self.__getattribute__(type)
        key = oss2key(oss_path)

        content = self.cache(type)
        if not content or not cache:
            content = read_object(key)
            self.cache(type, content)

        print "{type}: {oss_path}".format(type=type, oss_path=oss_path)
        if type == 'stdout':
            print dyeOKGREEN(content)
        elif type == 'stderr':
            print dyeWARNING(content)
        print '-' * 80

    @catchClientError
    def show_result(self, cache=True):
        result = self.cache('result')

        if self.deleted and not result:
            return
        elif result and cache:
            print result
            print '-' * 80
            return
        else:
            result = CLIENT.get_instance(self.id, self.name, 0).get('Result')

        if result.get('Detail') or result.get('ErrorCode'):
            self.cache('result', str(result))
            print dyeFAIL(str(result))
            print '-' * 80

    @catchClientError
    def show_job_message(self, cache=True):
        msg = self.cache('msg')
        if not msg or not cache:
            result = CLIENT.get_job(self.id)
            msg = result.get('Message')
            self.cache('msg', msg)
        if msg:
            print dyeFAIL(msg)
            print '-' * 80

    def debug(self, cache=True):
        print dyeOKBLUE("Task id: " + str(self.task.id))
        print dyeOKBLUE("Job id: " + self.id)
        self.show_log('stdout', cache)
        self.show_log('stderr', cache)
        self.show_result(cache)
        self.show_job_message(cache)

    def cache(self, type, content=None):
        def save_cache(content):
            with open(cache_file, 'w') as f:
                f.write(content)

        def read_cache():
            with open(cache_file, 'r') as f:
                content = f.read()
            return content

        def get_cache_path():
            cache_path = os.path.expanduser("~/.snap/cache")
            if not os.path.exists(cache_path):
                os.mkdir(cache_path)
            filename = "%s.%s" % (self.id, type)
            return  os.path.join(cache_path, filename)

        cache_file = get_cache_path()
        if content:
            save_cache(content)
        elif os.path.exists(cache_file):
            return read_cache()
        else:
            return None

class Instance(Base):
    __tablename__ = 'instance'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    cpu = Column(Integer, nullable=False)
    mem = Column(Integer, nullable=False)
    disk_type = Column(String)
    disk_size = Column(String)
    price = Column(Float, nullable=False)

    app = relationship("App", back_populates="instance")
    task = relationship("Task", back_populates="instance")
    bcs = relationship("Bcs", back_populates="instance")

    def __repr__(self):
        return "<Instance(id={id} name={name} cpu={cpu} mem={mem}, price={price})>".format(
	    id=self.id, name=self.name, cpu=self.cpu, mem=self.mem, price=self.price)

class Mapping(Base):
    __tablename__ = 'mapping'
    __table_args__ = (UniqueConstraint('name', 'source', 'destination', 'is_write', 'is_immediate'), )

    id = Column(Integer, primary_key=True)
    name = Column(String)
    source = Column(String)
    destination = Column(String)
    is_write = Column(Boolean, default=False)
    is_immediate = Column(Boolean, default=True)

    task = relationship("Task", secondary=task_mapping_table)

    def __repr__(self):
        return "<Mapping(id={id} {source}:{destination} write={is_write})>".format(
	    id=self.id, source=self.source, destination=self.destination, is_write=self.is_write)

    def oss_delete(self):
        key = oss2key(self.destination)
        BUCKET.delete_object(key)

    def size(self):
        key = oss2key(self.destination)
        return sum([obj.size for obj in ObjectIterator(BUCKET, prefix=key)])
