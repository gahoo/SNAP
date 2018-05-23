from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, ForeignKey, create_engine, Table
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError
from state_machine import *
from crontab import CronTab
from batchcompute.resources import (
    JobDescription, TaskDescription, DAG, AutoCluster, Networks,
    GroupDescription, ClusterDescription, Disks, Notification, )
from batchcompute.resources.cluster import Mounts, MountEntry
from batchcompute import ClientError
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkecs.request.v20140526 import DescribeSpotPriceHistoryRequest
from core.ali.bcs import CLIENT
from core.ali import ALI_CONF
from core.ali.oss import BUCKET, oss2key, OSSkeys, read_object
from core.formats import *
from core.misc import *
from core.notification.dingtalk import send_msg
from colorMessage import dyeWARNING, dyeFAIL, dyeOKGREEN
from collections import Counter
from oss2.exceptions import NoSuchKey
from oss2 import ObjectIterator
from argparse import Namespace
from flask import Flask
from jinja2 import Template
from StringIO import StringIO
import pandas as pd
import numpy as np
import functools
import getpass
import datetime
import time
import os
import json
import csv
import pdb
import sys
import re

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
    auto_scale = Column(Boolean, default=True)
    cluster = relationship("Cluster", uselist=False, back_populates="project")
    task = relationship("Task", back_populates="project")

    session = None
    logger = None
    message = []

    def __repr__(self):
        return "<Project(id={id}, name={name})>".format(id=self.id, name=self.name)

    def waited(self):
        return diff_date(get_date(self.create_date), get_date(self.start_date))

    def elapsed(self):
        return diff_date(get_date(self.start_date), get_date(self.finish_date))

    def sync(self):
        self.poll()
        if self.reach_max_jobs():
            print dyeWARNING('Reach max job limit')
            os._exit(0)

        to_sync = [t for t in self.task if t.is_waiting or t.is_running]
        to_check = [t for t in self.task if t.is_created or t.is_pending or t.is_failed]
        map(lambda x:x.check(), to_sync)
        map(lambda x:x.check(), to_check)

        if self.cluster and self.auto_scale:
            self.cluster.auto_scale()
        self.check_waiting_too_long(3600)
        self.check_instance_price(self.discount)
        self.notify()
        self.log_date()

    def log_date(self):
        if not self.start_date:
            self.start_date = datetime.datetime.now()
            self.save()
        all_finished = all([t.is_finished or t.is_cleaned for t in self.task])
        if not self.finish_date and all_finished:
            self.finish_date = datetime.datetime.now()
            self.save()

    def notify(self):
        is_work_time = datetime.datetime.now().hour in range(8, 19)
        if self.message and is_work_time:
            task_info = "\n".join(self.message)
            message = "**{project} ({progress}%)**\n{task}".format(project=self.name, progress=self.progress(), task=task_info)
            send_msg(message, title=self.name)

        all_finished = all([t.is_finished or t.is_cleaned for t in self.task])
        if not self.finish_date and all_finished and is_work_time:
            message = "**{project} (100%)**".format(project=self.name)
            send_msg(message, title=self.name)

    def poll(self):
        bcs = self.session.query(Bcs).filter( (Bcs.status=='Waiting') | (Bcs.status=='Running') ).all()
        map(lambda x:x.poll(), bcs)

    def check_waiting_too_long(self, timeout=3600):
        build_msg = lambda x, y: "- <{id}> *{sh}* has been waited for {time}".format(id=x.id, sh=os.path.basename(x.shell), time=y)
        bcs = self.session.query(Bcs).filter( (Bcs.status=='Waiting') ).all()
        msgs = [build_msg(b.task, b.waited()) for b in bcs if b.waited().total_seconds() > timeout]
        self.message.extend(msgs)

    def check_instance_price(self, max_discount=0.2):
        build_msg = lambda x, y: "- {instance}: {spot} / {origin} = {discount}".format(instance=x, spot=y[0], origin=y[1], discount=y[0]/y[1])
        bcs = self.session.query(Bcs).filter( (Bcs.status=='Running') ).all()
        instances = set([b.instance for b in bcs])
        prices = map(lambda x:x.latest_price(), instances)
        msgs = [build_msg(i.name, p) for i, p in zip(instances, prices) if p[0]/p[1] > max_discount]
        if msgs:
            msgs = ["\n\n*Folowing instance is getting expensive. Please consider switching alternative instance to avoid withdraw or high cost.*"] + msgs
            self.message.extend(msgs)

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

    def query_mappings(self, args, fuzzy=True):
        q = self.session.query(Mapping)
        if args.id:
            q = q.filter(Mapping.id.in_(args.id))
        if args.name:
            q = q.filter(Mapping.name == args.name)
        if args.source:
            if fuzzy:
                q = q.filter(Mapping.source.like("%" + args.source + "%"))
            else:
                q = q.filter(Mapping.source == args.source)
        if args.destination:
            if fuzzy:
                q = q.filter(Mapping.destination.like("%" + args.destination + "%"))
            else:
                q = q.filter(Mapping.destination == args.destination)
        if args.is_write is not None:
            q = q.filter(Mapping.is_write == args.is_write)
        if args.is_immediate is not None:
            q = q.filter(Mapping.is_immediate == args.is_immediate)
        if args.is_required is not None:
            q = q.filter(Mapping.is_required == args.is_required)
        mappings = q.all()
        return mappings

    def query_mapping_tasks(self, args):
        mappings = self.query_mappings(args)
        tasks = set(sum([m.task for m in mappings], []))
        return tasks

    def query_task_mappings(self,args):
        if args.task:
            args.id = args.task
        tasks = self.query_tasks(args)
        mappings = set(sum([t.mapping for t in tasks], []))

        kwargs = {k:v for k,v in args._get_kwargs() if k in ('name', 'source', 'destination', 'is_write', 'is_immediate', 'is_required', 'fuzzy') and v is not None}
        mappings = self.filter_mappings(mappings, **kwargs)
        return mappings

    def filter_mappings(self, mappings, **kwargs):
        is_fuzzy = kwargs.pop('fuzzy')
        for k in kwargs:
            if is_fuzzy:
                mappings = filter(lambda x: kwargs[k] in x.__getattribute__(k), mappings)
            else:
                mappings = filter(lambda x: x.__getattribute__(k) == kwargs[k], mappings)
        return mappings

    def query_bcs(self, args):
        bcs = self.session.query(Bcs).filter_by(id = args.job).one()
        return bcs

    def query_instance(self, args):
        q_filter = {k:v for k, v in args.__dict__.items() if v and k not in ('func', 'mode', 'project', 'latest')}
        q = self.session.query(Instance)
        if 'name' in q_filter:
            q = q.filter(Instance.name.like("%" + q_filter.pop('name') + "%"))
        q = q.filter_by(**q_filter)
        return q.all()

    def add_mapping(self, args):
        def get_task():
            if args.task:
                dummy_args = Namespace(id = None, status = None, shell = None, app = None, module = None)
                dummy_args.id = args.task
                tasks = self.query_tasks(dummy_args)
            else:
                tasks = []
            return tasks

        keys = ['name', 'source', 'destination', 'is_write', 'is_immediate', 'is_required']
        setting = {k:v for k,v in args._get_kwargs() if k in keys and v is not None}
        tasks = get_task()
        if not all(map(lambda x:x in setting, keys)):
            print dyeFAIL(', '.join(keys) + ' is required.')
            os._exit(1)

        m = Mapping(**setting)
        if tasks:
            m.task = tasks
        try:
            self.session.add(m)
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            mappings = self.query_mappings(args, fuzzy=False)
            print dyeFAIL('Following mapping already exists.')
            print format_mapping_tbl(mappings)

    def remove_mapping(self, args):
        def remove_mapping_task(mapping):
            affected_task.extend([t for t in mapping.task if t.id in args.task])
            mapping.task = [t for t in mapping.task if t.id not in args.task]

        def clean_mapping_task(mapping):
            mapping.task = []

        def get_mapping_tasks(mappings):
            tasks = []
            map(lambda x: tasks.extend(x.task), mappings)
            return set(tasks)

        def unlink_mapping_tasks(mappings, tasks):
            tids = " ".join([str(t.id) for t in tasks])
            msg = dyeWARNING('Unlink all related Task({tids})?[y/n]: '.format(tids=tids))
            if args.task:
                map(remove_mapping_task, mappings)
                self.session.commit()
                print dyeOKGREEN('Affected tasks:')
                print format_tasks_tbl(affected_task)
            elif tids and (args.yes or question(msg)):
                map(clean_mapping_task, mappings)
                self.session.commit()
                print dyeOKGREEN('Affected tasks:')
                print format_tasks_tbl(tasks)

        def delete_mappings(mappings_no_task):
            if mappings_no_task:
                mids = " ".join([str(m.id) for m in mappings_no_task])
                msg = "Mapping({mids}) without task will be deleted, proceed?[y/n]: ".format(mids=mids)
                if args.yes or question(dyeWARNING(msg)):
                    map(self.session.delete, mappings_no_task)
                    self.session.commit()
                    print dyeOKGREEN('Deleted Mappings:')
                    print format_mapping_tbl(mappings_no_task)

        mappings = self.query_mappings(args, fuzzy=args.fuzzy)
        tasks = get_mapping_tasks(mappings)
        affected_task = []
        unlink_mapping_tasks(mappings, tasks)
        mappings_no_task = [m for m in mappings if not m.task]
        delete_mappings(mappings_no_task)

    def count_active_jobs(self):
        return self.session.query(Bcs).filter(Bcs.status.in_(['Waiting', 'Running'])).count()

    def reach_max_jobs(self):
        return self.count_active_jobs() >= self.max_job

    def billing(self, billing_path):
        def date_dirs():
            dates = []
            if self.start_date:
                start = self.start_date
            else:
                print dyeFAIL("Project is not start yet.")
                os._exit(1)
            if self.finish_date:
                finish = self.finish_date + datetime.timedelta(days=1)
                finish = datetime.datetime.combine(finish, datetime.time.max)
            else:
                finish = datetime.datetime.combine(datetime.datetime.now(), datetime.time.max)

            i = start
            while i <= finish:
                dates.append(i.strftime("%Y-%m-%d"))
                i = i + datetime.timedelta(days=1)
            return dates

        def read_bill(billing_file):
            with open(billing_file, 'r') as billing_csv:
                bill_reader = csv.reader(billing_csv)
                map(add_cost, bill_reader)

        def add_cost(row):
            job_id = row[11].split('_')[0]
            cluster_id = row[11].split(';')[0]
            if job_id in bcs:
                bcs[job_id].cost += float(row[21])
            if cluster_id in clusters:
                clusters[cluster_id].cost += float(row[21])

        def add_cluster_cost(b):
            if b.elapsed():
                b.cost = clusters[b.cluster].cost * b.elapsed().total_seconds() / cluster_total_elapsed[b.cluster]

        def get_total_cluster_elapsed(bcs, clusters):
            total_elapsed = {c.id:[] for c in clusters}
            bcs_with_cluster = [b for b in bcs if b.cluster and b.elapsed()]
            map(lambda x: total_elapsed[x.cluster].append(x.elapsed().total_seconds()), bcs_with_cluster)
            total_elapsed = {k: sum(v) for k, v in total_elapsed.iteritems()}
            return total_elapsed

        def zero_cost(element):
            element.cost = 0

        if not self.finish_date:
            print dyeWARNING("Project not finished yet. Billing might be incompelte.")
        bcs = self.session.query(Bcs).all()
        clusters = self.session.query(Cluster).all()
        map(zero_cost, bcs)
        map(zero_cost, clusters)
        #project_elapsed = diff_date(self.start_date, self.finish_date)
        cluster_total_elapsed = get_total_cluster_elapsed(bcs, clusters)
        bcs = {b.id:b for b in bcs}
        clusters = {c.id:c for c in clusters}
        dates = date_dirs()
        for root, dirs, files in os.walk(billing_path):
            dirs[:] = [d for d in dirs if d in dates]
            for billing_file in files:
                if billing_file.endswith('.csv'):
                    read_bill(os.path.join(root, billing_file))
        map(add_cluster_cost, [b for b in bcs.values() if b.cluster])
        self.save()

    def cost_stat(self, mode):
        def make_each_cost(element):
            if 'name' in element.__dict__:
                name = element.name
            else:
                name = os.path.basename(element.shell)
            size = element.size(is_write=True)
            data_cost = round(size / (2.0 ** 30) * 0.148, 3)
            bcs_cost = element.cost()
            return (
                element.id,
                name,
                human_size(size),
                data_cost,
                bcs_cost,
                data_cost + bcs_cost)

        if mode == 'task':
            elements = self.task
        elif mode == 'app':
            elements = self.session.query(App).all()
        elif mode == 'module':
            elements = self.session.query(Module).all()

        return map(make_each_cost, elements)

    def cost(self):
        return sum([t.cost() for t in self.task])

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

            sizes = [n['data'].get(args.size, 1) for n in nodes]
            sizes = [s for s in sizes if s != 0 and s]
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
                if elapsed:
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
                data = app.size(is_write=True)
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
                data = module.size(is_write=True)
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
                min_start = choose_date(bcs, 'start_date', min)
                max_finish = choose_date(bcs, 'finish_date', max)
                elapsed = diff_date(min_start, max_finish)
                if elapsed:
                    elapsed = round(elapsed.total_seconds(), 0)
                else:
                    elapsed = 0
            return elapsed

        def choose_date(bcs, date_type, func):
            dates = [b.__getattribute__(date_type) for b in bcs if b.__getattribute__(date_type)]
            if dates:
                return func(dates)
            else:
                return None

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

    def profile(self, tasks):
        return pd.concat([t.profile() for t in tasks], ignore_index=True)

    def update(self, **kwargs):
        commom_keys = set(['name', 'description', 'owner', 'status', 'max_job', 'run_cnt', 'discount', 'email', 'mns', 'cluster', 'auto_scale']) & set(kwargs.keys())
        old_setting = [self.__getattribute__(k) for k in commom_keys]
        if 'cluster' in commom_keys and kwargs['cluster'] != '':
            self.bind_cluster(kwargs['cluster'])
        [self.__setattr__(k, kwargs[k]) for k in commom_keys if kwargs[k] != 'None' and k != 'cluster']
        [self.__setattr__(k, None) for k in commom_keys if kwargs[k] == 'None']
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
        deleted_size = human_size(self.size_stat(to_delete)['project'])
        print "{num} files({size}) deleted.".format(num=num_keys, size=deleted_size)

    def clean_bcs(self):
        bcs = self.session.query(Bcs).filter_by(deleted=False).all()
        map(lambda x:x.delete(), bcs)
        map(lambda x:x.delete_log(), bcs)
        map(lambda x:x.update(aasm_state = 'cleaned'), [t for t in self.task if t.is_finished])
        self.session.commit()
        print "{num} jobs deleted.".format(num=len(bcs))

    def size_stat(self, to_delete=None):
        total = 0
        clean_total = 0
        for obj in ObjectIterator(BUCKET, prefix="project/%s/" % self.name):
            if to_delete:
                if obj.key in to_delete:
                    total += obj.size
            else:
                total += obj.size

        for obj in ObjectIterator(BUCKET, prefix="clean/%s/" % self.name):
            clean_total += obj.size

        return {'clean': clean_total, 'project': total}

    def interactive_task(self, docker_image, inputs, outputs, instance_type, instance_image=None, cluster=None, timeout=60):
        def build_env():
            docker_oss_path = os.path.join('oss://', ALI_CONF['bucket'], ALI_CONF['docker_registry_oss_path']) + '/'
            return {
                "DEBUG": "TRUE",
                "TMATE_SERVER": ALI_CONF.get('tmate_server'),
                "BATCH_COMPUTE_DOCKER_IMAGE": "localhost:5000/" + docker_image,
                "BATCH_COMPUTE_DOCKER_REGISTRY_OSS_PATH": docker_oss_path
            }

        def prepare_cluster():
            cluster = AutoCluster()

            if instance_image is None:
                cluster.ImageId = ALI_CONF['default_image']
            else:
                cluster.ImageId = instance_image
            cluster.InstanceType = instance_type

            if cluster.InstanceType.startswith('bcs.'):
                cluster.ResourceType = "OnDemand"
            else:
                cluster.ResourceType = "Spot"
                cluster.SpotStrategy = "SpotAsPriceGo"

            cluster.Configs.Networks.VPC.CidrBlock = ALI_CONF['vpc_cidr_block']
            cluster.Configs.Networks.VPC.VpcId = ALI_CONF['vpc_id']

            return cluster

        def prepare_mapipngs(pairs):
            mappings = {}
            if pairs is None:
                return {}
            for pair in pairs:
                (source, destination) = pair.split(":", 1)
                mappings[source] = destination

            return mappings

        def prepare_task():
            task = TaskDescription()
            task.Parameters.Command.CommandLine = "sh -l -c 'sleep {timeout}'".format(timeout=timeout)
            task.Parameters.Command.EnvVars = build_env()
            task.Parameters.StdoutRedirectPath = "oss://{bucket}/project/{name}/log/".format(bucket=ALI_CONF['bucket'], name=self.name)
            task.Parameters.StderrRedirectPath = "oss://{bucket}/project/{name}/log/".format(bucket=ALI_CONF['bucket'], name=self.name)
            task.WriteSupport = True

            input_mapping = prepare_mapipngs(inputs)
            input_mapping.update({self.path: "oss://{bucket}/project/{name}/".format(bucket=ALI_CONF['bucket'], name=self.name)})
            task.Mounts.Entries = [MountEntry({'Source': oss, 'Destination': local, 'WriteSupport':True}) for local, oss in input_mapping.iteritems()]

            output_mapping = prepare_mapipngs(outputs)
            output_mapping.update({os.path.join(self.path, 'inspector') + '/': "oss://{bucket}/project/{name}/inspector/".format(bucket=ALI_CONF['bucket'], name=self.name)})
            task.OutputMapping = output_mapping

            task.Timeout = 86400 * 3
            task.MaxRetryCount = 0
            if cluster:
                task.ClusterId = cluster
            else:
                task.AutoCluster = prepare_cluster()

            return task

        def submit_job(task):
            job = JobDescription()
            job.Name = '{name}-inspector'.format(name=self.name)
            job.Description = '{name}-inspector'.format(name=self.name)
            job.DAG.add_task('inspector', task)
            job.Priority = 100
            job.AutoRelease = True
            return CLIENT.create_job(job).Id

        def check_stdout(id):
            log_id = "{id}.inspector.0".format(id = id)
            stdout = os.path.join(task.Parameters.StdoutRedirectPath, "stdout." + log_id)
            key = oss2key(stdout)
            content = read_object(key, (0, 5000), False)
            ssh = filter(lambda x:x.startswith('ssh'), content.split('\n'))
            return ssh

        def check_status(id):
            info = CLIENT.get_job(id)
            return info.State

        def wait4connect(id):
            print dyeOKBLUE('Inspector has been submit. Please wait until connection established. 1~5 min is normal. Make sure your docker image supports debug mode.')
            ssh = []
            progress = '\r'
            while not ssh:
                if check_status(id) == 'Running':
                    ssh = check_stdout(id)
                elif check_status(id) == 'Failed':
                    print dyeFAIL('\ninspect failed.')
                    print CLIENT.get_instance(id, 'inspector', 0).get('Result')
                    print CLIENT.get_job(id).get('Message')
                    return
                sys.stdout.write(progress)
                sys.stdout.flush()
                progress += '='
                time.sleep(15)
            os.system(ssh.pop())

        task = prepare_task()
        id = submit_job(task)
        wait4connect(id)
        if check_status(id) == 'Running':
            CLIENT.stop_job(id)
        CLIENT.delete_job(id)

    def create_cluster(self, image=None, instance=[], counts=[], mount_point=None, price_limit=[], disk_type=None, disk_size=None, vpc_id=None, vpc_cidr_block=None, **kwargs):
        def make_groups(instance, counts, price_limit):
            if not instance:
                instance = set([t.instance.name for t in self.task])
            if not counts:
                counts = [0] * len(instance)
            if not price_limit:
                price_limit = [None] * len(instance)
            groups = zip(instance, counts, price_limit)
            return [make_group_dict(instance, counts, price) for instance, counts, price in groups]

        def make_group_dict(instance, counts, price):
            instance = self.session.query(Instance).filter_by(name=instance).one()
            return {
                'name': instance.name,
                'instance': instance,
                'counts': counts,
                'price': price }

        groups = make_groups(instance, counts, price_limit)
        cluster = Cluster(name = self.name, disk_type=disk_type, disk_size=disk_size, project=self)
        cluster.create(groups=groups, mount_point=mount_point, discount=self.discount, instance_image=image, vpc_id=vpc_id, vpc_cidr_block=vpc_cidr_block)

    @catchClientError
    def bind_cluster(self, id):
        def bind_new(id):
            cluster = CLIENT.get_cluster(id)
            disks = cluster.Configs.Disks
            if disks.DataDisk.Size:
                disk_size = disks.DataDisk.Size
            else:
                disk_size = disks.SystemDisk.Size
            if disks.DataDisk.Type:
                disk_type = 'data.' + disks.DataDisk.Type
            elif disks.SystemDisk.Type:
                disk_type = 'system.' + disks.SystemDisk.Type
            else:
                disk_type = None
            return Cluster(id=id, name=cluster.Name, disk_type=disk_type, disk_size=disk_size, create_date=cluster.CreationTime, project=self)

        def bind_existed(id):
            return self.session.query(Cluster).filter_by(id=id).one()

        if self.session.query(Cluster).filter_by(id=id).all():
            bind_func = bind_existed
        else:
            bind_func = bind_new

        self.cluster = bind_func(id)
        self.save()


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

    def size(self, is_write=None):
        return sum([t.size(is_write=is_write) for t in self.task])

    def cost(self):
        return sum([t.cost() for t in self.task])

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

    def size(self, is_write=None):
        return sum([t.size(is_write=is_write) for t in self.task])

    def cost(self):
        return sum([t.cost() for t in self.task])

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
    docker_image = Column(String)
    disk_size = Column(Float)
    disk_type = Column(String)
    benchmark = Column(Boolean, default=True)
    debug_mode = Column(Boolean, default=False)
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
    kill = Event(from_states=(waiting, running, stopped), to_state=failed)
    retry = Event(from_states=failed, to_state=pending)
    redo = Event(from_states=(finished, cleaned), to_state=pending)
    clean = Event(from_states=(stopped, finished, failed), to_state=cleaned)

    def __repr__(self):
        return "<Task(id={id} sh={shell} status={status})>".format(id=self.id, shell=os.path.basename(self.shell), status=self.status)

    def msg(self, info):
        return "{id}\t{module}.{app}\t{sh}\t".format(id=self.id, module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell)) + info

    @after('start')
    @after('restart')
    @after('stop')
    @after('submit')
    @after('run')
    @after('finish')
    @after('fail')
    @after('kill')
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
           msg = self.msg("{old_state} => {state}".format(old_state=old_state, state=self.aasm_state))
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
        try:
            (task_name, task) = self.prepare_task()
        except Exception, e:
            msg = self.msg(str(e))
            self.project.logger.error(msg)
            task_info = "- <{id}> *{sh}* {status} | **FATAL ERROR**".format(id=self.id, sh=os.path.basename(self.shell), status=self.aasm_state)
            self.project.message.append(task_info)
            self.project.notify()
            raise

        if self.project.cluster and task.ClusterId:
            cluster_id = task.ClusterId
        else:
            cluster_id = None

        bcs = Bcs(
            name = task_name,
            spot_price_limit = task.AutoCluster.SpotPriceLimit,
            instance = self.instance,
            cluster = cluster_id)
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
            self.project.session.rollback()
            msg = self.msg(str(e))
            print dyeFAIL(msg)
            self.project.logger.error(msg)
            print bcs.job
            raise ClientError(e)
            self.fail()

    def prepare_task(self):
        task = TaskDescription()
        (script_name, ext) = os.path.splitext(os.path.basename(self.shell))
        script_name = script_name.replace('.', '_')
        oss_script_path = [m for m in self.mapping if m.name=='sh'][0].destination
        (oss_script_prefix, ext) = os.path.splitext(oss_script_path)
        oss_log_path = oss_script_prefix + '_log/'
        task.Parameters.Command.CommandLine = "sh -l {sh}".format(sh=self.shell)
        task.Parameters.Command.EnvVars = self.prepare_EnvVars()
        task.Parameters.StdoutRedirectPath = oss_log_path
        task.Parameters.StderrRedirectPath = oss_log_path
        task.WriteSupport = True

        #task.InputMapping = {m.source:m.destination for m in self.mapping if not m.is_write}
        task.OutputMapping = {m.source:m.destination for m in self.mapping if m.is_write}
        if self.benchmark:
            (oss_script_prefix, ext) = os.path.splitext(oss_script_path)
            task.OutputMapping['/var/log/pidstat.log'] = oss_script_prefix + '.{cnt}.pidstat'.format(cnt=len(self.bcs) + 1)
            task.OutputMapping['/var/log/du.log'] = oss_script_prefix + '.{cnt}.disk_usage'.format(cnt=len(self.bcs) + 1)
        #task.LogMapping = {m.source:m.destination for m in self.mapping if m.is_write}
        #task.Mounts.Entries = [MountEntry({'Source': m.destination, 'Destination': m.source, 'WriteSupport':m.is_write}) for m in self.mapping if not m.is_write]
        task.Mounts.Entries = self.prepare_Mounts()

        task.Timeout = 86400 * 3
        task.MaxRetryCount = 0
        if self.project.cluster and self.instance.name in self.get_cluster_instances() and not self.app.instance_image:
            task.ClusterId = self.project.cluster.id
        else:
            task.AutoCluster = self.prepare_cluster()
        return script_name, task

    def prepare_Mounts(self):
        def get_folder(path):
            if not path.endswith('/'):
                path = os.path.dirname(path) + '/'
            return path

        def is_nested(path, destination):
            for d in destination:
                if d.startswith(path) and d != path:
                    return True
            return False

        def check_nested(nested_func, destination):
            nested_flag = map(nested_func, destination)
            nested_mount = [dest for dest, flag in zip(destination, nested_flag) if flag]
            if nested_mount:
                msg = self.msg("Has Nested Mounts: {mount}".format(mount=nested_mount))
                self.project.logger.warning(msg)

            return nested_mount

        def is_input_exists(mapping):
            if not mapping.is_write:
                is_exist = mapping.exists()
            else:
                return True

            if not is_exist and mapping.is_required:
                raise IOError('%s not found on oss.' % mapping.destination)
            elif not is_exist and not mapping.is_required:
                msg = self.msg('%s not found on oss.' % mapping.destination)
                self.project.logger.warning(msg)
                print dyeWARNING(msg)
                return False
            elif is_exist and not mapping.destination.endswith('/') and mapping.size() == 0:
                msg = self.msg('%s size is zero.' % mapping.destination)
                self.project.logger.warning(msg)
                print dyeWARNING(msg)
            return True

        def fix_nested(mounts):
            def rm_nested_mounts(nested_path):
                return [m for m in mounts if not m.Destination.startswith(nested_path)]

            def new_nested_mount(nested_path):
                sources = [m.Source for m in mounts if m.Destination.startswith(nested_path)]
                write_supports = [m.WriteSupport for m in mounts if m.Destination.startswith(nested_path)]
                is_write = any(write_supports)
                all_write = all(write_supports)
                if all_write:
                    return None
                source = os.path.commonprefix(sources)
                if not source.endswith('/'):
                    source = os.path.dirname(source) + '/'
                common_suffix = os.path.commonprefix([source.strip('/')[::-1], nested_path.strip('/')[::-1]])
                if not common_suffix:
                    raise ValueError('Nested Mount fix might failed: %s %s' % (source, nested_path))

                return MountEntry({'Source': source, 'Destination': nested_path, 'WriteSupport':is_write})

            destinations = list(set([get_folder(m.Destination) for m in mounts]))
            duplicated_read_destinations = [get_folder(m.Destination) for m in mounts if not m.WriteSupport]
            read_destinations = set(duplicated_read_destinations)
            write_destinations = set([get_folder(m.Destination) for m in mounts if m.WriteSupport])
            # find read and write the same
            rw_destinations = list(read_destinations & write_destinations)
            # find nested read inside read
            is_read_destination_nested = functools.partial(is_nested, destination = read_destinations)
            nested_read_destinations = check_nested(is_read_destination_nested, read_destinations)
            # find duplicated read
            duplicated_read_destinations = set([x for x in duplicated_read_destinations if duplicated_read_destinations.count(x) > 1])
            duplicated_read_destinations = duplicated_read_destinations & set([m.Destination for m in mounts])
            duplicated_read_destinations = list(duplicated_read_destinations)
            # find nested write inside read
            is_write_destination_nested = functools.partial(is_nested, destination = write_destinations)
            nested_write_destinations = check_nested(is_write_destination_nested, read_destinations)

            nested_path_to_rm = list(set(duplicated_read_destinations + nested_read_destinations + nested_write_destinations + rw_destinations))
            nested_path_to_rm.sort()
            nested_mounts = []

            for nested_path in nested_path_to_rm:
                m = new_nested_mount(nested_path)
                mounts = rm_nested_mounts(nested_path)
                if m:
                    nested_mounts.append(m)

            entries = [m for m in mounts if not m.WriteSupport]
            entries.extend(nested_mounts)
            return entries

        mounts_entries = list(set([self.prepare_MountEntry(m) for m in self.mapping if is_input_exists(m)]))
        return fix_nested(mounts_entries)

    def prepare_MountEntry(self, mapping):
        source = mapping.destination
        destination = mapping.source
        return MountEntry({'Source': source, 'Destination': destination, 'WriteSupport':mapping.is_write})

    def prepare_EnvVars(self):
        env = {}
        if self.docker_image:
            docker_oss_path = os.path.join('oss://', ALI_CONF['bucket'], ALI_CONF['docker_registry_oss_path']) + '/'
            env.update({"BATCH_COMPUTE_DOCKER_IMAGE": "localhost:5000/" + self.docker_image,
                        "BATCH_COMPUTE_DOCKER_REGISTRY_OSS_PATH": docker_oss_path})
        if self.debug_mode:
            env['DEBUG'] = 'TRUE'
        if self.benchmark:
            env['BENCHMARK'] = 'TURE'
        tmate_server = ALI_CONF.get('tmate_server')
        if tmate_server:
            env['TMATE_SERVER'] = tmate_server
        benchmark_interval = ALI_CONF.get('benchmark_interval')
        if benchmark_interval:
            env['BENCHMARK_INTERVAL'] = benchmark_interval
        return env

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
            if self.project.discount > 0:
                cluster.SpotStrategy = "SpotWithPriceLimit"
            else:
                cluster.SpotStrategy = "SpotAsPriceGo"
            cluster.SpotPriceLimit = round(self.project.discount * self.instance.price, 3)

        cluster.Configs.Disks = self.prepare_disk()
        cluster.Notification = self.prepare_notify()
        cluster.Configs.Networks = self.prepare_network()

        return cluster

    def get_cluster(self):
        if self.project.cluster:
            return CLIENT.get_cluster(self.project.cluster.id)
        else:
            return None

    def get_cluster_instances(self):
        cluster_info = self.get_cluster()
        if cluster_info:
            return {v['InstanceType']:v['ActualVMCount'] for v in cluster_info.Groups.values()}
        else:
            return {}

    def prepare_disk(self):
        def get_common_prefix():
            prefix = os.path.commonprefix([m.source for m in self.mapping if m.is_write])
            if not prefix.endswith('/'):
                prefix = os.path.dirname(prefix) + '/'
            if prefix == '/':
                raise IOError("Invalid common prefix: /")
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
            disks.DataDisk.Size = int(self.disk_size)
            disks.DataDisk.MountPoint = get_common_prefix()

        def prepare_system_disk():
            if drive_type:
                disks.SystemDisk.Type = drive_type
            disks.SystemDisk.Size = 40 if self.disk_size <= 40 else int(self.disk_size)

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
        if self.debug_mode or 'ne' in self.instance.name:
            network.VPC.CidrBlock = ALI_CONF['vpc_cidr_block']
            network.VPC.VpcId = ALI_CONF['vpc_id']
        return network

    @before('restart')
    def restart_task(self):
        bcs = self.bcs[-1]
        bcs.restart()
        msg = self.msg("stopped => pending")
        self.project.logger.info(msg)

    @before('stop')
    def stop_task(self):
        bcs = self.bcs[-1]
        bcs.stop()
        bcs.finish_date = datetime.datetime.now()
        msg = self.msg("{state} => stopped".format(state=self.aasm_state))
        self.project.logger.info(msg)

    @before('clean')
    def delete_tasks(self):
        map(lambda x:x.delete(), self.bcs)
        msg = self.msg("{state} => cleaned".format(state=self.aasm_state))
        self.project.logger.info(msg)

    @before('kill')
    def kill_tasks(self):
        self.bcs[-1].delete()
        self.bcs[-1].finish_date = datetime.datetime.now()
        msg = self.msg("{state} => killed".format(state=self.aasm_state))
        self.project.logger.info(msg)

    @before('clean')
    @before('redo')
    @before('retry')
    def delete_files(self):
        oss_files = [m for m in self.mapping if m.is_immediate and m.is_write]
        map(lambda x:x.oss_delete(), oss_files)

    def show_json(self, cache=True):
        # json style
        bcs = self.bcs[-1]
        bcs.show_json(cache)

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

    def show_mapping_tbl(self, size=False):
        if self.mapping:
            mids = " ".join([str(m.id) for m in self.mapping])
            print dyeOKGREEN("File Mappings: " + mids)
            print format_mapping_tbl(self.mapping, size)

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
        def update_mapping(mappings):
            if mappings:
                return self.project.session.query(Mapping).filter(Mapping.id.in_(mappings)).all()
            else:
                return []

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

        if 'mappings' in kwargs:
            self.mapping = update_mapping(kwargs.pop('mappings'))
            print dyeOKGREEN("Mappings Changed:")
            print format_mapping_tbl(self.mapping)

        commom_keys = set(['cpu', 'mem', 'docker_image', 'disk_size', 'disk_type', 'debug_mode', 'benchmark', 'aasm_state']) & set(kwargs.keys())
        old_setting = [self.__getattribute__(k) for k in commom_keys]
        [self.__setattr__(k, kwargs[k]) for k in commom_keys]
        [self.__setattr__(k, None) for k in commom_keys if kwargs[k] == 'None']
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

    @after('finish')
    def check_output(self):
        def check_each_output(m):
            if not m.exists():
                msg = self.msg('{oss} is not found.'.format(oss=m.destination))
                print dyeWARNING(msg)
                self.project.logger.warning(msg)
            else:
                if m.size() == 0:
                    msg = self.msg('{oss} has 0 size.'.format(oss=m.destination))
                    print dyeWARNING(msg)
                    self.project.logger.warning(msg)

        output_mappings = [m for m in self.mapping if m.is_write and m.is_required]
        map(check_each_output, output_mappings)

    def size(self, is_write=None):
        if is_write is None:
            return sum([m.size() for m in self.mapping])
        else:
            return sum([m.size() for m in self.mapping if m.is_write  == is_write])

    def cost(self):
        return sum([b.cost for b in self.bcs])

    @after('submit')
    @after('retry')
    @after('redo')
    def reset_finish_date(self):
        if self.project.finish_date:
            self.project.finish_date = None
            self.save()

    def attach(self):
        if self.aasm_state not in ['waiting', 'running']:
            print dyeWARNING('Task status is not running.')
        elif not self.debug_mode:
            print dyeWARNING('debug_mode is not True.')
        elif not self.bcs:
            print dyeWARNING('No task submited yet.')
        elif self.debug_mode and self.bcs:
            bcs = self.bcs[-1]
            bcs.attach()

    @after('fail')
    def enqueue_failed_message(self):
        if self.reach_max_failed(3):
            msg = "- <{id}> *{sh}* {status} | [detail](#)".format(id=self.id, module=self.module.name, app=self.app.name, sh=os.path.basename(self.shell), status=self.aasm_state)
            self.project.message.append(msg)

    def profile(self):
        def load_disk_usage(key):
            content = read_object(key, full=True)
            du = pd.read_table(StringIO(content), delim_whitespace=True)
            return pd.DataFrame({
                'sys': du[du.Mounted == '/'].reset_index().Used.astype('int64'),
                'data': du[du.Filesystem == '/dev/xvdb1'].reset_index().Used.astype('int64'),
                'file': os.path.basename(key).rstrip('.disk_usage') })

        def add_time_disk_usage(ps, du):
            if ps is None:
                return None
            times = ps.Time.unique()
            lack_num = len(du) - len(times)
            if len(times) <= 1:
                step = times[0] + 60000000000
            else:
                step = times[1]
            if lack_num > 0:
                lack = times[-1] + step * np.arange(1, lack_num + 1)
                times = np.append(times, lack)
            elif lack_num < 0:
                times = times[:lack_num]
            du['Time'] = times
            du['sys'] = du['sys'] - du['sys'][0]
            return du

        def process_pidstat_line(line, cmd_idx):
            elements = line.strip().split()
            return "\t".join(elements[:cmd_idx]) + '\t' + " ".join(elements[cmd_idx:])

        def extract_command(cmd):
            match = pattern.search(cmd)
            if match:
                return match.group()
            else:
                return os.path.basename(cmd.split()[0])

        def normalize_time(times, date):
            if isinstance(times[0], str):
                times = date + times
                times = pd.to_datetime(times)
                times = times - times[0]
            elif times.dtype == 'int64':
                times = pd.to_datetime(times, unit='s')
                times = times - times[0]
            else:
                print "other type"
                pdb.set_trace()
            return times

        def load_pidstat(key):
            content = read_object(key, full=True)
            lines = content.split('\n')
            if len(lines) < 4:
                return None
            date = lines[0].split('\t')[1]
            headers = [l for l in lines[:6] if l.startswith('#')].pop()
            headers = headers.lstrip('#').split()
            n_column = len(headers)
            cmd_idx = headers.index('Command')

            lines = [process_pidstat_line(l, cmd_idx) for l in lines if not l.startswith('Linux') and l != '' and not l.startswith('#')]
            lines = [l for l in lines if l[:8].isdigit() or l[2] == ':']
            lines = [l for l in lines if len(l.strip('\t').split('\t')) == n_column]
            content = "\n".join(lines)
            ps = pd.read_table(StringIO(content), sep='\t', header=None, names=headers)

            ps['Program'] = map(extract_command, ps.Command)
            ps['file'] = os.path.basename(key).rstrip('.pidstat')
            ps = ps[~ps.Program.isin(['cron', 'CRON', 'crond', 'pidstat'])].reset_index()
            ps.Time = normalize_time(ps.Time, date)

            return ps

        sys.stdout.write('processing task %s\r' % self.id)
        sys.stdout.flush()
        pattern = re.compile(r'\w+\.(jar|R|pl|py)')
        sh = filter(lambda x:x.name =='sh', self.mapping)[0]
        key = oss2key(sh.destination.rstrip('sh'))
        related_files = [obj.key for obj in ObjectIterator(BUCKET, prefix=key)]
        pidstats = filter(lambda x:x.endswith('pidstat'), related_files)
        disk_usages = filter(lambda x:x.endswith('disk_usage'), related_files)
        if len(pidstats) != len(disk_usages):
            raise ValueError('{id}\tThe number of pidstats and disk_usages is differ'.format(id=self.id))
        if not pidstats or not disk_usages:
            return None

        pidstats = map(load_pidstat, pidstats)
        disk_usages = map(load_disk_usage, disk_usages)
        disk_usages = map(lambda x:add_time_disk_usage(*x), zip(pidstats, disk_usages))
        profiles = pd.merge(pd.concat(pidstats), pd.concat(disk_usages), how='left')
        profiles['App'] = self.app.name
        profiles['Module'] = self.module.name
        profiles['Instance'] = self.instance.name
        profiles['Instance.CPU'] = self.instance.cpu
        profiles['Instance.MEM'] = self.instance.mem
        profiles['Instance.Disk'] = 40 if self.disk_size <= 40 else int(self.disk_size)
        return profiles

class Bcs(Base):
    __tablename__ = 'bcs'

    id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    deleted = Column(Boolean, default=False)
    cluster = Column(String)
    spot_price = Column(Float)
    stdout = Column(String) # Path
    stderr = Column(String)
    create_date = Column(DateTime, default=datetime.datetime.now())
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    spot_price_limit = Column(Float)
    cost = Column(Float, default=0)
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

    def waited(self):
        return diff_date(get_date(self.create_date), get_date(self.start_date))

    def elapsed(self):
        return diff_date(get_date(self.start_date), get_date(self.finish_date))

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

    def attach(self):
        key = oss2key(self.stdout)
        content = read_object(key, (0, 5000))
        ssh = filter(lambda x:x.startswith('ssh'), content.split('\n'))
        if ssh:
            os.system(ssh.pop())
        else:
            print dyeWARNING('Is debug mode really on? Is this image has tmate? Or maybe task is still waiting')

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

    def history_price(self, day=None):
        client = AcsClient(ALI_CONF['accesskey_id'], ALI_CONF['accesskey_secret'], ALI_CONF['region'])
        request = DescribeSpotPriceHistoryRequest.DescribeSpotPriceHistoryRequest()
        request.set_NetworkType('vpc')
        request.set_InstanceType(self.name)
        if day:
            utc_now = datetime.datetime.utcnow()
            utc_start = utc_now - datetime.timedelta(days=day)
            request.set_StartTime(utc_start.strftime("%Y-%m-%dT%H:%M:%SZ"))
            request.set_EndTime(utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"))
        try:
            response = client.do_action_with_exception(request)
        except ServerException, e:
            print dyeFAIL(str(e) + " Instance Type: %s" % self.name)
            return []
        prices = json.loads(response)
        return prices['SpotPrices']['SpotPriceType']

    def latest_price(self):
        prices = self.history_price()
        if prices:
            (SpotPrice, OriginPrice) = (prices[-1]['SpotPrice'], prices[-1]['OriginPrice'])
        else:
            (SpotPrice, OriginPrice) = (None, None)
        return SpotPrice, OriginPrice

class Mapping(Base):
    __tablename__ = 'mapping'
    __table_args__ = (UniqueConstraint('name', 'source', 'destination', 'is_write', 'is_immediate'), )

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    source = Column(String, nullable=False)
    destination = Column(String, nullable=False)
    is_write = Column(Boolean, nullable=False, default=False)
    is_immediate = Column(Boolean, nullable=False, default=True)
    is_required = Column(Boolean, nullable=False, default=True)

    task = relationship("Task", secondary=task_mapping_table)

    def __repr__(self):
        return "<Mapping(id={id} {source}:{destination} write={is_write})>".format(
	    id=self.id, source=self.source, destination=self.destination, is_write=self.is_write)

    def oss_delete(self, recursive=False):
        key = oss2key(self.destination)
        is_exists = BUCKET.object_exists(key)
        if is_exists:
            BUCKET.delete_object(key)
        elif not is_exists and recursive:
            keys = [obj.key for obj in ObjectIterator(BUCKET, prefix=key)]
            BUCKET.batch_delete_objects(keys)

    def size(self):
        key = oss2key(self.destination)
        return sum([obj.size for obj in ObjectIterator(BUCKET, prefix=key)])

    def source_size(self):
        if os.path.exists(self.source):
            if os.path.isdir(self.source):
                return get_folder_size(self.source)
            else:
                return os.path.getsize(self.source)
        else:
            return 0

    def exists(self):
        key = oss2key(self.destination)
        is_exists = BUCKET.object_exists(key)
        if not is_exists:
            try:
                ObjectIterator(BUCKET, prefix=key).next()
                return True
            except StopIteration:
                return False
        else:
            return is_exists

    def show_detail_tbl(self, size=False):
        print dyeOKGREEN("Mapping details:")
        print format_detail_mapping(self, size)

    def show_task_tbl(self):
        if self.task:
            tids = " ".join([str(t.id) for t in self.task])
            print dyeOKGREEN("Related Tasks: " + tids)
            print format_tasks_tbl(self.task)

    def update(self, **kwargs):
        task_updated = ''
        affected_task = []
        affected_task.extend(self.task)
        if 'task' in kwargs:
            old_tids = " ".join([str(t.id) for t in self.task])
            self.task = kwargs.pop('task')
            tids = " ".join([str(t.id) for t in self.task])
            affected_task.extend(self.task)
            task_updated = "task:\t{old} => {new}".format(old=old_tids, new=tids)

        commom_keys = set(['name', 'source', 'destination', 'is_write', 'is_immediate', 'is_required']) & set(kwargs.keys())
        old_setting = [self.__getattribute__(k) for k in commom_keys]
        [self.__setattr__(k, kwargs[k]) for k in commom_keys]
        kwargs = {k:kwargs[k] for k in commom_keys}
        updated = "\n".join(["%s:\t%s => %s" % (k, old, new) for k, old, new in zip(commom_keys, old_setting, kwargs.values())])
        print "\n".join([dyeOKGREEN("Mapping {id} updated:").format(id = self.id), updated, task_updated])
        if affected_task:
            print dyeOKGREEN('Affected tasks:')
            print format_tasks_tbl(set(affected_task))

    def sync(self, overwrite=False):
        if self.is_write:
            self.download(overwrite)
        else:
            self.upload(overwrite)

    def download(self, overwrite=False):
        self._ossutil(self.destination, self.source, overwrite)

    def upload(self, overwrite=False):
        self._ossutil(self.source, self.destination, overwrite)

    def _ossutil(self, source, destination, overwrite=False):
        def is_path_exists(path):
            if path.startswith('oss://'):
                return self.exists()
            else:
                return os.path.exists(path)

        def dye_path(is_exists, path):
            if is_exists:
                return dyeOKGREEN(path)
            else:
                return dyeFAIL(path)

        def is_dir(path):
            return path.endswith('/') or (not path.startswith('oss://') and os.path.isdir(path))

        def traversal_key(source, destination):
            if source.startswith('oss://') and source.endswith('/'):
                key = oss2key(source)
                sources = [obj.key for obj in ObjectIterator(BUCKET, prefix=key)]
                sources = filter(lambda x:not x.endswith('/'), sources)
                destinations = [destination + s.replace(key, '') for s in sources]
                sources = ["oss://{bucket}/{key}".format(bucket=BUCKET.bucket_name, key=s) for s in sources]
            else:
                sources = [source]
                destinations = [destination]
            return zip(sources, destinations)

        def cp(src, dest):
            cmdline = " ".join(cmd + [src, dest])
            os.system(cmdline)

        cmd = ['ossutil', 'cp']
        if not source.startswith('oss://') and is_dir(source):
            cmd.append('-r')
        if overwrite:
            cmd.append('-f')

        is_source_exists = is_path_exists(source)
        is_destination_exists = is_path_exists(destination)
        msg = 'Mapping({id}) %s: {source} => {dest}'.format(
          id=self.id,
          dest=dye_path(is_destination_exists, destination),
          source=dye_path(is_source_exists, source))

        if (overwrite or not is_destination_exists) and is_source_exists:
            print dyeOKBLUE(msg % 'Syncing')
            [cp(src, dest) for src, dest in traversal_key(source, destination)]
        else:
            print dyeWARNING(msg % 'Skipped')


class Cluster(Base):
    __tablename__ = 'cluster'

    id = Column(String, primary_key=True)
    name = Column(String)
    disk_size = Column(Float)
    disk_type = Column(String)
    create_date = Column(DateTime, default=datetime.datetime.now())
    finish_date = Column(DateTime)
    cost = Column(Float, default=0)

    project_id = Column(Integer, ForeignKey('project.id'), nullable=True)
    project = relationship("Project", back_populates="cluster")

    def __repr__(self):
        return "<Cluster(id={id})>".format(id=self.id)

    def elapsed(self):
        return diff_date(get_date(self.create_date), get_date(self.finish_date))

    def save(self):
        self.project.save()

    def create(self, **kwargs):
        cluster_desc = self.prepare_cluster(**kwargs)
        try:
            self.id = CLIENT.create_cluster(cluster_desc).Id
            self.save()
        except ClientError, e:
            print cluster_desc
            msg = str(e)
            print dyeFAIL(msg)
            self.project.logger.error(msg)

    def prepare_cluster(self, groups, mount_point=None, discount=0.1, instance_image=None, vpc_id=None, vpc_cidr_block=None):
        add_groups = lambda x:cluster_desc.add_group(x['name'].replace('.', '-'), self.prepare_group(discount=discount, **x))

        cluster_desc = ClusterDescription()
        cluster_desc.Name = self.name
        map(add_groups, groups)

        cluster_desc.Configs.Disks = self.prepare_disk(mount_point=mount_point)
        cluster_desc.Configs.Networks = self.prepare_network(vpc_id=vpc_id, vpc_cidr_block=vpc_cidr_block)
        cluster_desc.Notification = self.prepare_notify()

        if instance_image:
            cluster_desc.ImageId = instance_image
        else:
            cluster_desc.ImageId = ALI_CONF['default_image']

        return cluster_desc

    def prepare_group(self, name, instance, counts, price, discount=0.1):
        group_desc = GroupDescription()
        group_desc.DesiredVMCount = counts
        group_desc.InstanceType = instance.name
        if instance.name.startswith('bcs.'):
            group_desc.ResourceType = 'OnDemand'
        elif price > 0:
            group_desc.ResourceType = 'Spot'
            group_desc.SpotStrategy = 'SpotWithPriceLimit'
            group_desc.SpotPriceLimit = price
        elif price == 0:
            group_desc.ResourceType = 'Spot'
            group_desc.SpotStrategy = 'SpotAsPriceGo'
        elif price is None:
            group_desc.ResourceType = 'Spot'
            group_desc.SpotStrategy = 'SpotWithPriceLimit'
            group_desc.SpotPriceLimit = round(discount * instance.price, 3)
        else:
            msg = 'Cluster({cluster}) invalid {name} settings: {instance} x {counts} <= {price}'
            msg = msg.format(cluster=self.id, name=name, instance=instance.name, counts=counts, price=price)
            raise ValueError(msg)

        return group_desc

    def prepare_disk(self, mount_point=None):
        def prepare_data_disk():
            if drive_type:
                disks.DataDisk.Type = drive_type
            disks.DataDisk.Size = int(self.disk_size)
            if mount_point:
                disks.DataDisk.MountPoint = mount_point
            else:
                disks.DataDisk.MountPoint = self.project.path

        def prepare_system_disk(size):
            if drive_type:
                disks.SystemDisk.Type = drive_type
            if size <40:
                disks.SystemDisk.Size = 40
            else:
                disks.SystemDisk.Size = size

        disks = Disks()
        if not self.disk_size:
           return disks

        if self.disk_type:
            (disk_type, drive_type) = self.disk_type.split('.')
        else:
            (disk_type, drive_type) = ('system', None)

        if self.disk_size > 500:
            disk_type = 'data'
        elif self.disk_size <= 40:
            disk_type = 'system'

        if disk_type == 'data':
            prepare_system_disk(40)
            prepare_data_disk()
        elif disk_type == 'system':
            prepare_system_disk(self.disk_size)

        return disks

    def prepare_network(self, vpc_id=None, vpc_cidr_block=None):
        network = Networks()
        if vpc_id:
            network.VPC.VpcId = vpc_id
        else:
            network.VPC.VpcId = ALI_CONF['vpc_id']

        if vpc_cidr_block:
            network.VPC.CidrBlock = vpc_cidr_block
        else:
            network.VPC.CidrBlock = ALI_CONF['vpc_cidr_block']

        return network

    def prepare_notify(self):
        notice = Notification()
        if self.project.mns:
            notice.Topic.Endpoint = self.project.mns
            notice.Topic.Name = self.project.name
            notice.Topic.Events = ['OnJobFailed', 'OnTaskFailed', 'OnInstanceFailed']

        return notice

    def count_instance(self):
        cnts = dict()
        cluster = CLIENT.get_cluster(self.id)
        return {g.InstanceType: {'actual': g.ActualVMCount, 'desired': g.DesiredVMCount} for g in cluster.Groups.values()}

    @catchClientError
    def scale(self, **groups):
        CLIENT.change_cluster_desired_vm_count(self.id, **groups)

    def auto_scale(self):
        def count_instance(instance):
            bcs_instance = [b for b in bcs if b.instance.name == instance]
            status = {'running': len([b for b in bcs_instance if b.status=='Running']),
                      'waiting': len([b for b in bcs_instance if b.status=='Waiting' and b.waited().total_seconds() > 600])}
            status.update(cluster_status[instance])
            return status

        def count_instance_status(instance, status):
            return len([b for b in bcs if b.status==status and b.instance.name == instance])

        def calc_desired(running, waiting, actual, desired):
            sufficiency = desired - (running + waiting)
            # scale up
            if sufficiency < 0:
                new_desired = desired + 1 + ((abs(sufficiency) - 1)  / 3)
            # scale down
            elif sufficiency > 0:
                new_desired = desired - 1 - (sufficiency  / 2)
            else:
                new_desired = desired
            return new_desired

        cluster_status = self.count_instance()
        bcs = self.project.session.query(Bcs).filter( ((Bcs.status=='Waiting') | (Bcs.status=='Running')) & (Bcs.cluster is not None) ).all()
        bcs = [b for b in bcs if b.instance.name in cluster_status]
        instances = cluster_status.keys()
        instance_status = dict(zip(instances, map(count_instance, instances)))
        new_desired = map(lambda x:calc_desired(**x), instance_status.values())
        group_name = map(lambda x: x.replace('.', '-'), instance_status.keys())
        groups = dict(zip(group_name, new_desired))
        self.scale(**groups)

        old_desired = [status['desired'] for status in instance_status.values()]
        scaled_instance = filter(lambda x:x[1] != x[2], zip(instance_status.keys(), old_desired, new_desired))
        msg = map(lambda x: "%s: %s -> %s" % x, scaled_instance)
        if msg:
            self.project.message.extend(["\n\n*Cluster auto scaled:*\n"] + msg)
            self.project.logger.info('\t'.join(msg))
