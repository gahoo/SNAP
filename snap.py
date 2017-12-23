#!/usr/bin/python

import argparse
import yaml
import sys
import os
import logging
import pdb
import functools
import json
from core.app import App
from core.pipe import WorkflowParameter
from core.pipe import Pipe
from core import models
from core.formats import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from crontab import CronTab
from flask import Flask
from jinja2 import Template

def loadYaml(filename):
    with open(filename, 'r') as yaml_file:
        return yaml.load(yaml_file)

def dumpYaml(filename, obj):
    with open(filename, 'w') as yaml_file:
        yaml.dump(obj, yaml_file, default_flow_style=False)

def new_app(args):
    if args.name:
        app = App(args.name)
        app.new()
    else:
        subparsers_app_new.print_help()
        print >> sys.stderr, "app name is not optional."

def init_app(args):
    if args.name:
        app = App(args.name)
    elif(args.config):
        name = os.path.dirname(args.config)
        app = App(name)
        app.config_file = args.config
    else:
        # subparsers_app_build.print_help()
        print >> sys.stderr, "app name or config.yaml must be specified."
        os._exit(0)
    return app

def build_app(args):
    app = init_app(args)
    app.build(parameter_file=args.param, dependence_file=args.depend, debug=args.debug, output=args.out)

def node_app(args):
    app = init_app(args)
    if args.type == 'all':
        app.buildTestWorkflow(args.out)
    else:
        nodes = app.nodes(args.type)
        if(args.node):
            # select node
            app.dumpYaml([nodes[args.node]], args.out)
        else:
            app.dumpYaml(nodes.values(), args.out)

def parameter_pipe(args):
    if args.name:
        parameter = WorkflowParameter(args.name, args.project_path, args.values)
        parameter.render(args.out)
    else:
        print >> sys.stderr, "workflow name must be specified."
        os._exit(0)

def build_pipe(args):
    if args.pipe_path and os.path.isdir(args.pipe_path):
        pipe = Pipe(args.pipe_path)
    else:
        print >> sys.stderr, "Pipeline path is invalid"
        os._exit(0)

    if args.param:
        pipe.build(parameter_file=args.param,
                   proj_path=args.out,
                   pymonitor_path=args.pymonitor_path,
                   proj_name=args.proj_name,
                   queue=args.queue,
                   priority=args.priority)
    else:
        print >> sys.stderr, "parameters.conf is missing."
        os._exit(0)

def config_bcs(args):
    ali_conf_file = os.path.expanduser("~/.snap/ali.conf")
    conf = {}
    if os.path.exists(ali_conf_file):
        conf = loadYaml(ali_conf_file)
    new_conf = {k:v for k,v in args._get_kwargs() if v and k != 'func'}
    conf.update(new_conf)
    dumpYaml(ali_conf_file, conf)

def sync_bcs(args):
    if not args.project:
        projects = [load_project(name, dbfile) for name, dbfile in db.items()]
        map(lambda x: x.sync(), projects)
    else:
        project = load_project(args.project, db[args.project])
        project.sync()

def stat_bcs(args):
    if not args.project:
        projects = [load_project(name, dbfile) for name, dbfile in db.items()]
        #map(lambda x: x.states(), projects)
        #projects = {p.name:p.states() for p in projects}
    else:
        projects = [load_project(args.project, db[args.project])]
        #print project.states()

    print format_project_tbl(projects, args.size)

def cron_bcs(args):
    def get_job():
        try:
            job = cron.find_comment(args.project).next()
        except StopIteration:
            job = None
        return job

    cron  = CronTab(user=True)
    if not args.project:
        for job in cron:
            print job
        return

    job = get_job()
    if args.add and not job:
        (snap_path, ext) = os.path.splitext(os.path.realpath(__file__))
        command = "{snap} bcs sync -p {project}".format(snap=snap_path, project=args.project)
        job  = cron.new(command=command, comment=args.project)
        job.minute.every(args.interval)
        cron.write()
        print "cron job %s added." % args.project
    if args.add and job:
        job.minute.every(args.interval)
        cron.write()
        print "cron job %s updated." % args.project
    elif args.delete and job:
        cron.remove_all(comment=args.project)
        cron.write()
        print "cron job %s deleted." % args.project
    elif not job:
        msg = "cron job %s not Found" % args.project
        print dyeFAIL(msg)
    else:
        print job

def clean_bcs(args):
    proj = load_project(args.project, db[args.project])
    proj.clean_files(immediate = not args.all_files)
    proj.clean_bcs()

def instance_bcs(args):
    if args.project:
        proj = load_project(args.project, db[args.project])
    elif db:
        proj = load_project(db.keys()[0], db.values()[0])
    else:
        print "You must have at least one project to query instance."
        os._exit(1)
    instances = proj.query_instance(args)
    print format_instance_tbl(instances).get_string(sortby="price")

def load_project(name, dbfile):
    session = new_session(name, dbfile)
    proj = session.query(models.Project).filter_by(name = name).one()
    proj.session = session
    proj.logger = new_log(name, dbfile)
    return proj

def new_session(name, dbfile):
    engine = create_engine('sqlite:///' + dbfile)
    Session = sessionmaker(bind=engine)
    return Session()

def new_log(name, dbfile):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    (prefix, ext) =  os.path.splitext(dbfile)
    log_file = prefix + '.log'
    fh = logging.FileHandler(log_file)
    fmt = "%(asctime)-15s\t%(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def list_task(args):
    proj = load_project(args.project, db[args.project])
    if args.source or args.destination:
        tasks = proj.query_mapping_tasks(args)
    else:
        tasks = proj.query_tasks(args)
    print format_tasks_tbl(tasks).get_string(sortby="create", reversesort=True)

def show_task(args):
    def show_each_task(task):
        task.show_detail_tbl()
        if args.jobs:
            task.show_bcs_tbl(args.instance)
        if args.mappings:
            task.show_mapping_tbl()
        if args.depends:
            task.show_depends_tbl()
        if args.script:
            task.show_shell()

    proj = load_project(args.project, db[args.project])
    tasks = proj.query_tasks(args)
    map(show_each_task, tasks)

def debug_task(args):
    proj = load_project(args.project, db[args.project])
    if args.job:
        bcs = proj.query_bcs(args)
        bcs.debug(args.cache)
        if args.json:
            bcs.show_json(args.cache)
    else:
        tasks = proj.query_tasks(args)
        map(lambda x:x.debug(args.cache), tasks)
        if args.json:
            map(lambda x:x.show_json(args.cache), tasks)

def update_task(args):
    setting = {k:v for k,v in args._get_kwargs() if k in ('instance', 'cpu', 'mem', 'disk_type', 'disk_size') and v}
    if args.state:
        setting['aasm_state'] = args.state

    proj = load_project(args.project, db[args.project])
    tasks = proj.query_tasks(args)
    if setting and tasks:
        map(lambda x: x.update(**setting), tasks)
        tasks[0].project.session.commit()
        print "Changes commited."

def do_task(args, status, event):
    args.status = status
    proj = load_project(args.project, db[args.project])
    tasks = proj.query_tasks(args)
    ids = ", ".join(map(lambda x: str(x.id), tasks))
    status = " or ".join(args.status)

    if not args.id and tasks and not args.yes:
        msg = "{status} task ({ids}) will {event}, proceed?[y/n]: ".format(status=status, ids=ids, event=event)
        confirm = raw_input(dyeWARNING(msg))
        if confirm not in ['y', 'yes']:
            os._exit(0)

    if tasks:
        map(lambda x: x.__getattribute__(event)(), tasks)
        msg = 'Task {ids} will be {event}.'.format(ids=ids, event=event)
        print msg
    else:
        msg = "No task will {event} since no {status} task found.".format(event=event, status=status)
        print dyeFAIL(msg)

restart_task = functools.partial(do_task, status = ['stopped'], event = 'restart')
retry_task = functools.partial(do_task, status = ['failed'], event = 'retry')
redo_task = functools.partial(do_task, status = ['finished'], event = 'redo')
stop_task = functools.partial(do_task, status = ['pending', 'waiting', 'running'], event = 'stop')
clean_task = functools.partial(do_task, status = ['stopped', 'finished', 'failed'], event = 'clean')

def submit_task(args):
    proj = load_project(args.project, db[args.project])
    tasks = proj.query_tasks(args)
    setting = {'aasm_state': 'pending'}
    map(lambda x: x.update(**setting), tasks)
    do_task(args, ['pending'], 'submit')

def cyto_task(args):
    app = Flask(__name__)
    @app.route('/')
    def network():
        template_file = os.path.join(snap_path, 'cyto', 'network.html')
        template = Template(open(template_file).read())

        task_status = app_status = module_status = ''

        if args.mode == 'task':
            task_status = build_status_css()
        if args.mode == 'app':
            app_status = build_status_css()
        if args.mode == 'module':
            module_status = build_status_css()

        return template.render(
          edges=json.dumps(edges),
          nodes=json.dumps(nodes),
          layout=args.layout,
          task_status = task_status,
          app_status = app_status,
          module_status = module_status,
          size=args.size)

    def build_status_css():
        colors = ['#74CBE8', '#f5ff6b', '#E8747C', '#74E883', '#74E883']
        states = ['running', 'stopped', 'failed', 'finished', 'cleaned']
        css = {'pie-size': '80%'}
        for (i, (color, state)) in enumerate(zip(colors, states)):
            css['pie-%s-background-color' % (i + 1)] = color
            css['pie-%s-background-size' % (i + 1)] = 'mapData(%s, 0, 1, 0, 100)' % state
        return ",\n".join(["'{k}': '{v}'".format(k=k, v=v) for k, v in css.items()])

    snap_path = os.path.dirname(os.path.realpath(__file__))
    proj = load_project(args.project, db[args.project])
    (edges, nodes) = proj.build_network(args)
    app.run(host='0.0.0.0', port=args.port)

if __name__ == "__main__":
    parsers = argparse.ArgumentParser(
        description = "SNAP is Not A Pipeline.",
        version = "0.1",
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parsers.add_subparsers()

    #app
    parsers_app = subparsers.add_parser('app',
        help = "Operations of APP.",
        description = "New, Build, Test, Run",
        prog = 'snap app',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_app = parsers_app.add_subparsers()
    #app new
    subparsers_app_new = subparsers_app.add_parser('new',
        help='new APP template',
        description="",
        prog='snap app new',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_app_new.add_argument('-name', help = "app name")
    subparsers_app_new.set_defaults(func=new_app)
    #app build
    subparsers_app_build = subparsers_app.add_parser('build',
        help='build APP template',
        description="This command can render config.yaml into *.sh with parameter.yaml file."
        "Without `-param`, default value in config.yaml will be used to render scripts."
        "`-name` or `-config` argument must be specified. "
        "Without `-out`, script will be writed to STDOUT",
        prog='snap app build',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_app_build.add_argument('-name', help = "app name")
    subparsers_app_build.add_argument('-config', help = "config.yaml file")
    subparsers_app_build.add_argument('-param', help = "render from parameter.yaml file. default will be use if not specified.")
    subparsers_app_build.add_argument('-depend', help = "render defaults from dependencies.yaml file. ")
    subparsers_app_build.add_argument('-debug', action='store_true', help = "show debug render info.")
    subparsers_app_build.add_argument('-out', help = "output render result to file. default write to stdout")
    subparsers_app_build.set_defaults(func=build_app)
    #app node
    subparsers_app_node = subparsers_app.add_parser('node',
        help='APP workflow node template',
        description="This command can render config.yaml into workflow nodes."
        "Without `-out`, result will be writed to STDOUT",
        prog='snap app node',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_app_node.add_argument('-name', help = "app name")
    subparsers_app_node.add_argument('-config', help = "config.yaml file")
    subparsers_app_node.add_argument('-type', choices=['all', 'app', 'load', 'store'],
        help = "`all`: test workflow; "
        "`app`: app node only; "
        "`load`: load node only; "
        "`store`: store node only; ")
    subparsers_app_node.add_argument('-node',
        help = "select node to output"
        "load: inputs name"
        "store: outputs name"
        "app: app name")
    subparsers_app_node.add_argument('-out', help = "output render result to file. default write to stdout")
    subparsers_app_node.set_defaults(func=node_app)
    #pipe
    parsers_pipe = subparsers.add_parser('pipe',
        help = "Operations of PIPE.",
        description = "New, Build, Test, Run",
        prog = 'snap pipe',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe = parsers_pipe.add_subparsers()
    # pipe parameter
    subparsers_pipe_parameter = subparsers_pipe.add_parser('parameter',
        help='PIPE workflow parameter template',
        description="This command can render template.yaml into workflow parameters."
        "Without `-out`, result will be writed to STDOUT",
        prog='snap pipe parameter',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_parameter.add_argument('-name', help="pipe name")
    subparsers_pipe_parameter.add_argument('-project_path', help="project output path in parameter", default='testPIPE/RNA_ref')
    subparsers_pipe_parameter.add_argument('-values', help="file contains values to be replace in template.yaml")
    subparsers_pipe_parameter.add_argument('-out', help="output render result to file. default write to stdout")
    subparsers_pipe_parameter.set_defaults(func=parameter_pipe)

    # pipe build
    subparsers_pipe_build = subparsers_pipe.add_parser('build',
        help='PIPE build shell and dependencies',
        description="This command render all app config.yaml into *.sh with parameters.conf. "
        "Build dependencies and everything needed to run a pipe",
        prog='snap pipe build',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_build.add_argument('-pipe_path', help="the path to the pipeline")
    subparsers_pipe_build.add_argument('-proj_name', help = "the name of project")
    subparsers_pipe_build.add_argument('-param', help = "render from parameter.yaml file. default will be use if not specified.")
    subparsers_pipe_build.add_argument('-pymonitor_path', help = "path to pymonitor", default='/data/pipeline/RNA_pipeline/RNA_ref/RNA_ref_v1.0/software/monitor')
    subparsers_pipe_build.add_argument('-priority', help = "priority of qsub", default='RD_test')
    subparsers_pipe_build.add_argument('-queue', help = "queue of qsub", default='all.q')
    subparsers_pipe_build.add_argument('-out', help="output everything needed for a project")
    subparsers_pipe_build.set_defaults(func=build_pipe)

    # bcs
    parsers_bcs = subparsers.add_parser('bcs',
        help = "Operations of BCS.",
        description = "Start Pause Sync Stats Clean Task",
        prog = 'snap bcs',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs = parsers_bcs.add_subparsers()
    # bcs config
    subparsers_bcs_config = subparsers_bcs.add_parser('config',
        help='Configure Aliyun BCS.',
        description="This command will configure Aliyun BCS settings.",
        prog='snap bcs config',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_config.add_argument('-accesskey_id', help="accesskey_id for Aliyun BCS.")
    subparsers_bcs_config.add_argument('-accesskey_secret', help="accesskey_secret for Aliyun BCS.")
    subparsers_bcs_config.add_argument('-bucket', help="bucket to save results.")
    subparsers_bcs_config.add_argument('-region', help="which Aliyun BCS region you are.")
    subparsers_bcs_config.add_argument('-image', default='img-ubuntu', help="defualt instance image to run BCS.")
    subparsers_bcs_config.add_argument('-registry_path', default='docker-images', help="docker registry path on bucket.")
    subparsers_bcs_config.set_defaults(func=config_bcs)

    # bcs stat
    subparsers_bcs_stat = subparsers_bcs.add_parser('stat',
        help='Show Project task stats and progress.',
        description="This command will show stats about projects",
        prog='snap bcs stat',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_stat.add_argument('-project', default=None, help="ContractID or ProjectID, default will show all project recorded in ~/.snap/db.yaml")
    subparsers_bcs_stat.add_argument('-size', action='store_true', help="Project data usage stat")
    subparsers_bcs_stat.add_argument('-cost', action='store_true', help="Project costs")
    subparsers_bcs_stat.set_defaults(func=stat_bcs)
    # bcs sync
    subparsers_bcs_sync = subparsers_bcs.add_parser('sync',
        help='Sync and update task states with Aliyun BCS.',
        description="This command will poll and sync task states from Aliyun BCS",
        prog='snap bcs sync',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_sync.add_argument('-project', default=None, help="ContractID or ProjectID, sync all project in ~/.snap/db.yaml")
    subparsers_bcs_sync.set_defaults(func=sync_bcs)

    # bcs cron
    subparsers_bcs_cron = subparsers_bcs.add_parser('cron',
        help='Set Crontab for Aliyun BCS.',
        description="This command will modify crontab config to sync with Aliyun BCS",
        prog='snap bcs cron',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_cron.add_argument('-project', default=None, help="ContractID or ProjectID you want to schedule")
    subparsers_bcs_cron.add_argument('-interval', default=15, help="sync interval in minute")
    bcs_cron_mutually_group = subparsers_bcs_cron.add_mutually_exclusive_group()
    bcs_cron_mutually_group.add_argument('-add', action='store_true', help="add crontab job")
    bcs_cron_mutually_group.add_argument('-delete', action='store_true', help="del crontab job")
    subparsers_bcs_cron.set_defaults(func=cron_bcs)

    # bcs clean
    subparsers_bcs_clean = subparsers_bcs.add_parser('clean',
        help='Clean Jobs and Files on Aliyun BCS.',
        description="This command will clean files and jobs on Aliyun BCS",
        prog='snap bcs clean',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_clean.add_argument('-project', default=None, required=True, help="ContractID or ProjectID you want to clean")
    subparsers_bcs_clean.add_argument('-all_files', default=False, action='store_true', help="Delete all output files or just immediate files")
    subparsers_bcs_clean.set_defaults(func=clean_bcs)

    # bcs instance
    subparsers_bcs_instance = subparsers_bcs.add_parser('instance',
        help='Show available instances.',
        description="This command will clean files and jobs on Aliyun BCS",
        prog='snap bcs instance',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_instance.add_argument('-project', default=None, help="ContractID or ProjectID you want to clean")
    subparsers_bcs_instance.add_argument('-name', help="instance name")
    subparsers_bcs_instance.add_argument('-cpu', type=int, help="how many core")
    subparsers_bcs_instance.add_argument('-mem', type=float, help="memory size")
    subparsers_bcs_instance.add_argument('-disk_type', choices=('SSD', 'HDD'), help="disk type: SSD/HDD")
    subparsers_bcs_instance.add_argument('-disk_size', help="local disk size")
    subparsers_bcs_instance.add_argument('-price', type=float, help="instance price")
    subparsers_bcs_instance.set_defaults(func=instance_bcs)
    #bcs task
    #subparsers_bcs_task = subparsers_bcs.add_parser('task',
    #    help='Sync and update task states with Aliyun BCS.',
    #    description="This command will poll and sync task states from Aliyun BCS",
    #    prog='snap bcs task',
    #    formatter_class=argparse.RawTextHelpFormatter)
    #subparsers_bcs_task.add_argument('-project', default=None, help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    #subparsers_bcs_task.set_defaults(func=sync_bcs)

    # task
    parsers_task = subparsers.add_parser('task',
        help = "Operations of tasks",
        description = "Start Pause Sync Stats Clean Task",
        prog = 'snap task',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task = parsers_task.add_subparsers()

    # task select common args
    share_task_parser = argparse.ArgumentParser(add_help=False)
    share_task_parser.add_argument('-project', required=True, help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    share_task_parser.add_argument('-id', default=None, help="Task id", nargs="*", type = int)
    share_task_parser.add_argument('-shell', default='.', help="Task shell")
    share_task_parser.add_argument('-status', default=None, help="Task status", nargs="*")
    share_task_parser.add_argument('-app', default=None, help="Task app")
    share_task_parser.add_argument('-module', default=None, help="Task module")
    share_task_parser.add_argument('-yes', action='store_true', help="Don't ask.")

    #task list
    subparsers_task_list = subparsers_task.add_parser('list',
        help='List tasks on BCS.',
        description="This command will print tasks whith certain critieria",
        prog='snap task list',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_list.add_argument('-source', default=None, help="Task with source mapping")
    subparsers_task_list.add_argument('-destination', default=None, help="Task with destination mapping")
    subparsers_task_list.add_argument('-is_write', default=None, dest='write', action='store_true', help="This is a writable mapping")
    subparsers_task_list.add_argument('-is_not_write', default=None, dest='write', action='store_false', help="This is not a writable mapping")
    subparsers_task_list.add_argument('-is_immediate', default=None, dest='immediate', action='store_true', help="This is a immediate mapping")
    subparsers_task_list.add_argument('-is_not_immediate', default=None, dest='immediate', action='store_false', help="This is not a immediate mapping")
    subparsers_task_list.set_defaults(func=list_task)

    #task show
    subparsers_task_show = subparsers_task.add_parser('show',
        help='Show tasks detail.',
        description="This command will print task detail",
        prog='snap task show',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_show.add_argument('-jobs', default=False, action='store_true', help="Show jobs or not")
    subparsers_task_show.add_argument('-instance', default=False, action='store_true', help="Show instance detail or not")
    subparsers_task_show.add_argument('-mappings', default=False, action='store_true', help="Show mappings or not")
    subparsers_task_show.add_argument('-depends', action='store_true', help="Show depends or not")
    subparsers_task_show.add_argument('-script', action='store_true', help="Show script on oss")
    subparsers_task_show.set_defaults(func=show_task)

    #task debug
    subparsers_task_debug = subparsers_task.add_parser('debug',
        help='Show task log or json to help debugging',
        description="This command will print task logs and related messages.",
        prog='snap task debug',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_debug.add_argument('-json', action='store_true', help="Show job json")
    subparsers_task_debug.add_argument('-job', help="Bcs job id want to check")
    subparsers_task_debug.add_argument('-no-cache', dest='cache', default=True, action='store_false', help="use cache or not.")
    subparsers_task_debug.set_defaults(func=debug_task)

    #task update
    subparsers_task_update = subparsers_task.add_parser('update',
        help='Update task configure such as instance and disk usage etc.',
        description="This command will update task configure, multiple task is supported.",
        prog='snap task update',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_update.add_argument('-instance', help="Update task instance")
    subparsers_task_update.add_argument('-cpu', help="Update task cpu", type=int)
    subparsers_task_update.add_argument('-mem', help="Update task mem", type=float)
    subparsers_task_update.add_argument('-disk_type', help="Update task disk type")
    subparsers_task_update.add_argument('-disk_size', help="Update task disk size", type=float)
    subparsers_task_update.add_argument('-state', help="Update task status")
    subparsers_task_update.set_defaults(func=update_task)

    #task restart
    subparsers_task_restart = subparsers_task.add_parser('restart',
        help='Restart selected stopped tasks.',
        description="This command will restart selected stopped tasks.",
        prog='snap task restart',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_restart.set_defaults(func=restart_task)

    #task retry
    subparsers_task_retry = subparsers_task.add_parser('retry',
        help='Retry selected failed tasks.',
        description="This command will retry selected failed tasks.",
        prog='snap task retry',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_retry.set_defaults(func=retry_task)

    #task redo
    subparsers_task_redo = subparsers_task.add_parser('redo',
        help='Redo selected finished tasks.',
        description="This command will redo selected finished tasks.",
        prog='snap task redo',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_redo.set_defaults(func=redo_task)

    #task stop
    subparsers_task_stop = subparsers_task.add_parser('stop',
        help='Stop selected pending, waiting, running tasks.',
        description="This command will stop selected pending, waiting, running tasks.",
        prog='snap task stop',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_stop.set_defaults(func=stop_task)

    #task clean
    subparsers_task_clean = subparsers_task.add_parser('clean',
        help='Clean selected stopped, finished, failed tasks, including bcs job and immediate output files',
        description="This command will clean selected stopped, finished, failed tasks, including bcs job and immediate output files",
        prog='snap task clean',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_clean.set_defaults(func=clean_task)

    #task submit
    subparsers_task_clean = subparsers_task.add_parser('submit',
        help='Submit task immediately and ignore all restraint.',
        description="This command will submit task immediately and ignore all restraint.",
        prog='snap task submit',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_clean.set_defaults(func=submit_task)

    #task cyto
    subparsers_task_cyto = subparsers_task.add_parser('cyto',
        help='Show selected task in cytoscape.js',
        description="This command will show selected task dependencies in network.",
        prog='snap task cyto',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_cyto.add_argument('-mode', default='task', choices=('task', 'app', 'module'), help="Update task instance")
    subparsers_task_cyto.add_argument('-layout', default='breadthfirst', help="Network layout")
    subparsers_task_cyto.add_argument('-port', default=8000, type=int, help="Port expose")
    subparsers_task_cyto.add_argument('-compound', default='all', choices=('app', 'module', 'all', 'none'), help="Port expose")
    subparsers_task_cyto.add_argument('-size', default='elapsed', choices=('elapsed', 'cpu', 'mem', 'data'), help="What does size map")
    subparsers_task_cyto.set_defaults(func=cyto_task)



    # bcs cron
    # bcs cron add
    # bcs cron remove
    # bcs task
    # bcs task update
    # bcs task restart
    # bcs task retry
    # bcs task redo
    # bcs task stop
    # bcs task clean
    # bcs task submit
    # bcs task log
    # bcs task show
    # bcs clean
    # bcs instance
    # bcs deliver
    # bcs archive

if __name__ == '__main__':
    argslist = sys.argv[1:]
    db_yaml = os.path.expanduser("~/.snap/db.yaml")
    if not os.path.exists(db_yaml):
        open(db_yaml, 'a').close()
    db = loadYaml(db_yaml)
    if len(argslist) > 0:
        args = parsers.parse_args(argslist)
        args.func(args)
    else:
        parsers.print_help()
        os._exit(0)
