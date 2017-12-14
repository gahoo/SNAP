#!/usr/bin/python

import argparse
import yaml
import sys
import os
import logging
import pdb
from core.app import App
from core.pipe import WorkflowParameter
from core.pipe import Pipe
from core import models
from core.formats import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def loadYaml(filename):
    with open(filename, 'r') as yaml_file:
        return yaml.load(yaml_file)

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

    print format_project_tbl(projects)

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
    fmt = "%(asctime) %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
    datefmt = "%a %d %b %Y %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def load_tasks(args):
    def set_session(task):
        task.project.session = session

    session = new_session(args.project, db[args.project])
    q = session.query(models.Task)
    if args.id:
        q = q.filter(models.Task.id.in_(args.id))
    if args.status:
        q = q.filter(models.Task.aasm_state.in_(args.status))
    if args.shell:
        q = q.filter(models.Task.shell.like("%" + args.shell + "%"))
    if args.app:
        q = q.join(models.App).filter(models.App.name == args.app)
    if args.module:
        q = q.join(models.Module).filter(models.Module.name == args.module)
    tasks = q.all()
    if tasks:
        map(set_session, tasks)
    return tasks

def load_mapping_tasks(args):
    session = new_session(args.project, db[args.project])
    q = session.query(models.Mapping)
    if args.source:
        q = q.filter(models.Mapping.source.like("%" + args.source + "%"))
    if args.destination:
        q = q.filter(models.Mapping.destination.like("%" + args.destination + "%"))
    mappings = q.all()
    tasks = set(sum([m.task for m in mappings], []))
    return tasks

def list_task(args):
    if args.source or args.destination:
        tasks = load_mapping_tasks(args)
    else:
        tasks = load_tasks(args)
    print format_tasks_tbl(tasks).get_string(sortby="start", reversesort=True)

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

    tasks = load_tasks(args)
    map(show_each_task, tasks)

def debug_task(args):
    if args.job:
        bcs = load_bcs(args)
        bcs.debug()
        if args.json:
            bcs.show_json()
    else:
        tasks = load_tasks(args)
        map(lambda x:x.debug(), tasks)
        if args.json:
            map(lambda x:x.show_json(), tasks)

def load_bcs(args):
    session = new_session(args.project, db[args.project])
    bcs = session.query(models.Bcs).filter_by(id = args.job).one()
    return bcs

def update_task(args):
    setting = {k:v for k,v in args._get_kwargs() if k in ('instance', 'cpu', 'mem', 'disk_type', 'disk_size') and v}
    tasks = load_tasks(args)
    if setting and tasks:
        map(lambda x: x.update(**setting), tasks)
        tasks[1].project.session.commit()
        print "Changes commited."

def restart_task(args):
    args.status = ['stopped']
    tasks = load_tasks(args)
    ids = ", ".join(map(lambda x: x.id, tasks))
    if not args.id and tasks:
        confirm = raw_input(dyeWARNING("All stopped task (%s) will restart, proceed?[y/n]: " % ids))
        if confirm not in ['y', 'yes']:
            os._exit(0)
    if tasks:
        map(lambda x: x.restart(), tasks)
        print "Task " + ids + ' will be restart.'
    else:
        print dyeFAIL("No task restart since no stopped task found.")

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
    # bcs stat
    subparsers_bcs_stat = subparsers_bcs.add_parser('stat',
        help='Show Project task stats and progress.',
        description="This command will show stats about projects",
        prog='snap bcs stat',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_stat.add_argument('-project', default=None, help="ContractID or ProjectID, default will show all project recorded in ~/.snap/db.yaml")
    subparsers_bcs_stat.set_defaults(func=stat_bcs)
    # bcs sync
    subparsers_bcs_sync = subparsers_bcs.add_parser('sync',
        help='Sync and update task states with Aliyun BCS.',
        description="This command will poll and sync task states from Aliyun BCS",
        prog='snap bcs sync',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_sync.add_argument('-project', default=None, help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    subparsers_bcs_sync.set_defaults(func=sync_bcs)

    #bcs task
    subparsers_bcs_task = subparsers_bcs.add_parser('task',
        help='Sync and update task states with Aliyun BCS.',
        description="This command will poll and sync task states from Aliyun BCS",
        prog='snap bcs task',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_task.add_argument('-project', default=None, help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    subparsers_bcs_task.set_defaults(func=sync_bcs)

    # task
    parsers_task = subparsers.add_parser('task',
        help = "Operations of tasks",
        description = "Start Pause Sync Stats Clean Task",
        prog = 'snap task',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task = parsers_task.add_subparsers()

    # task select common args
    share_task_parser = argparse.ArgumentParser(add_help=False)
    share_task_parser.add_argument('-project', help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    share_task_parser.add_argument('-id', default=None, help="Task id", nargs="*", type = int)
    share_task_parser.add_argument('-shell', default='.', help="Task shell")
    share_task_parser.add_argument('-status', default=None, help="Task status", nargs="*")
    share_task_parser.add_argument('-app', default=None, help="Task app")
    share_task_parser.add_argument('-module', default=None, help="Task module")

    #task list
    subparsers_task_list = subparsers_task.add_parser('list',
        help='List tasks on BCS.',
        description="This command will print tasks whith certain critieria",
        prog='snap task list',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_list.add_argument('-source', default=None, help="Task with source mapping")
    subparsers_task_list.add_argument('-destination', default=None, help="Task with destination mapping")
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
        help='Show task log or json.',
        description="This command will print task logs",
        prog='snap task debug',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_debug.add_argument('-json', action='store_true', help="Show job json")
    subparsers_task_debug.add_argument('-job', help="Bcs job id want to check")
    subparsers_task_debug.set_defaults(func=debug_task)

    #task update
    subparsers_task_update = subparsers_task.add_parser('update',
        help='Show task log or json.',
        description="This command will print task logs",
        prog='snap task debug',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_update.add_argument('-instance', help="Update task instance")
    subparsers_task_update.add_argument('-cpu', help="Update task cpu", type=int)
    subparsers_task_update.add_argument('-mem', help="Update task mem", type=float)
    subparsers_task_update.add_argument('-disk_type', help="Update task disk type")
    subparsers_task_update.add_argument('-disk_size', help="Update task disk size", type=float)
    subparsers_task_update.set_defaults(func=update_task)

    #task restart
    subparsers_task_restart = subparsers_task.add_parser('restart',
        help='Show task log or json.',
        description="This command will print task logs",
        prog='snap task debug',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_restart.set_defaults(func=restart_task)



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
    # bcs task run
    # bcs task log
    # bcs task show
    # bcs clean
    # bcs instance

if __name__ == '__main__':
    argslist = sys.argv[1:]
    db_yaml = os.path.expanduser("~/.snap/db.yaml")
    db = loadYaml(db_yaml)
    if len(argslist) > 0:
        args = parsers.parse_args(argslist)
        args.func(args)
    else:
        parsers.print_help()
        os._exit(0)
