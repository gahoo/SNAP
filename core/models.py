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
import getpass
import datetime
import os
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

    def startAll(self):
        [t.start() for t in self.task if t.is_created]

    def sync(self):
        self.poll()

        to_sync = [t for t in self.task if t.is_waiting or t.is_running]
        to_check = [t for t in self.task if t.is_created or t.is_pending or t.is_failed]
        map(lambda x:x.check(), to_sync)
        map(lambda x:x.check(), to_check)

    def poll(self):
        bcs = self.session.query(Bcs).filter( (Bcs.status=='Waiting') | (Bcs.status=='Running') ).all()
        map(lambda x:x.poll(), bcs)

    def states(self):
        states = [t.aasm_state for t in self.task]
        return Counter(states)

    def state_filter(self, state):
        return self.session.query(Task).filter_by(aasm_state = state).all()
        #return [t for t in self.task if t.aasm_state == state]

    def retry(self, id):
        task = self.session.query(Task).filter_by(id = id).one()
        task.retry()

    def redo(self, id):
        task = self.session.query(Task).filter_by(id = id).one()
        task.redo()

    def update(self, id, **kwargs):
        task = self.session.query(Task).filter_by(id = id).one()
        task.update(**kwargs)

    def debug(self, id):
        task = self.session.query(Task).filter_by(id = id).one()
        task.debug()

    def cleanImmediate(self):
        immediate_write = [m.destination for m in self.session.query(Mapping).filter_by(is_write = True, is_immediate = True).all()]
        all_read = [m.destination for m in self.session.query(Mapping).filter_by(is_write = False).all()]
        to_delete = set(all_read) & set(immediate_write)
        oss_keys = OSSkeys(map(oss2key, to_delete))
        for keys in oss_keys:
            result = BUCKET.batch_delete_objects(keys)
            print('\n'.join(result.deleted_keys))

    def cleanBcs(self, status=None):
        if status:
            bcs = self.session.query(Bcs).filter( Bcs.status==status ).all()
        else:
            bcs = self.session.query(Bcs).all()
        map(lambda x:x.delete(), bcs)
        self.session.commit()


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
           msg = "{module}.{app}\t{sh}: {old_state} => {state}".format(module=self.module.name, app=self.app.name,
                sh=os.path.basename(self.shell), old_state=old_state, state=self.aasm_state)
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
        msg = "{module}.{app}\t{sh}: stopped => pending".format(module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell))
        self.project.logger.info(msg)

    @before('stop')
    def stop_task(self):
        bcs = self.bcs[-1]
        bcs.stop()

    @before('clean')
    def delete_tasks(self):
        map(lambda x:x.delete(), self.bcs)

    @before('clean')
    @before('redo')
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

    def debug(self):
        bcs = self.bcs[-1]
        bcs.debug()

    def show_detail_tbl(self):
        print dyeOKGREEN("Task Details:")
        print format_detail_task(self)

    def show_bcs_tbl(self, with_instance):
        if self.bcs:
            print dyeOKGREEN("Jobs on bcs:")
            print format_bcs_tbl(self.bcs, with_instance)

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
    def show_json(self):
        json = self.cache('json')
        if not json:
            json = CLIENT.get_job_description(self.id)
            self.cache('json', json)
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
        CLIENT.delete_job(self.id)
        self.deleted = True

    def show_log(self, type):
        oss_path = self.__getattribute__(type)
        key = oss2key(oss_path)

        content = self.cache(type)
        if not content:
            content = read_object(key)
            self.cache(type, content)

        print "{type}: {oss_path}".format(type=type, oss_path=oss_path)
        if type == 'stdout':
            print dyeOKGREEN(content)
        elif type == 'stderr':
            print dyeWARNING(content)
        print '-' * 80

    @catchClientError
    def show_result(self):
        result = self.cache('result')
        if self.deleted and not result:
            return
        if not result:
            result = CLIENT.get_instance(self.id, self.name, 0).get('Result')
            self.cache('result', result)
        if result.get('Detail') or result.get('ErrorCode'):
            print dyeFAIL(str(result))
            print '-' * 80

    @catchClientError
    def show_job_message(self):
        msg = self.cache('msg')
        if not msg:
            result = CLIENT.get_job(self.id)
            msg = result.get('Message')
            self.cache('msg', msg)
        if msg:
            print dyeFAIL(msg)
            print '-' * 80

    def debug(self):
        print dyeOKBLUE("Task id: " + str(self.task.id))
        print dyeOKBLUE("Job id: " + self.id)
        self.show_log('stdout')
        self.show_log('stderr')
        self.show_result()
        self.show_job_message()

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
    disk_size = Column(Float)
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
