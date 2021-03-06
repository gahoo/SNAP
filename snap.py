#!/usr/bin/python

import argparse
import sys
import os
import logging
import pdb
import functools
import commands
from core.app import App
from core.pipe import WorkflowParameter
from core.pipe import Pipe
from core import models
from core.formats import *
from core.misc import *
from core.db import DB
from core.ali.price import app as price_app
from core.gantt import app as gantt_app
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from crontab import CronTab

def yaml2dict(filepath):
    if not os.path.exists(filepath):
        open(filepath, 'a').close()
    return loadYaml(filepath) or dict()

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

def run_app(args):
    kwargs = {k:v for k,v in args._get_kwargs() if k != 'func' and v is not None}
    app = init_app(args)
    app.run(**kwargs)

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

def add_pipe(args):
    if args.name:
        pipe_name = args.name
    else:
        pipe_name = os.path.basename(args.url).rstrip('.git')

    pipe_path = os.path.join(args.destination, pipe_name)
    pipe = Pipe(pipe_path)
    tag = pipe.add(args.url, args.overwrite)
    installed_pipe[pipe_name] = {'path': pipe_path, 'tag': tag}
    dumpYaml(pipe_yaml, installed_pipe)

def list_pipe(args):
    def add_oss_path(pipe_name, pipeline):
        pipeline['oss'] = os.path.join('oss://', ali_conf['bucket'], ali_conf['pipeline_path'], pipe_name, pipeline['tag'])

    ali_yaml = os.path.expanduser("~/.snap/ali.conf")
    ali_conf = yaml2dict(ali_yaml)
    filtered_pipe = {k:v for k, v in installed_pipe.items() if not args.name or args.name in k}
    map(add_oss_path, filtered_pipe.keys(), filtered_pipe.values())
    print format_pipe_tbl(filtered_pipe)

def switch_pipe(args):
    check_pipe_exists(args.name)
    pipe_path = installed_pipe[args.name]['path']
    pipe = Pipe(pipe_path)
    pipe.switch(args.version)
    installed_pipe[args.name]['tag'] = args.version
    dumpYaml(pipe_yaml, installed_pipe)

def upgrade_pipe(args):
    check_pipe_exists(args.name)
    pipe_path = installed_pipe[args.name]['path']
    pipe = Pipe(pipe_path)
    tag = pipe.upgrade(refspec=args.refspec)
    installed_pipe[args.name]['tag'] = tag
    dumpYaml(pipe_yaml, installed_pipe)

def deploy_pipe(args):
    check_pipe_exists(args.name)
    pipe_path = installed_pipe[args.name]['path']
    pipe = Pipe(pipe_path)
    ali_yaml = os.path.expanduser("~/.snap/ali.conf")
    ali_conf = yaml2dict(ali_yaml)
    pipe.deploy(ali_conf['pipeline_path'], args.version)

def remove_pipe(args):
    check_pipe_exists(args.name)
    pipe_path = installed_pipe[args.name]['path']
    pipe = Pipe(pipe_path)
    if args.aliyun:
        ali_yaml = os.path.expanduser("~/.snap/ali.conf")
        ali_conf = yaml2dict(ali_yaml)
        pipe.destroy(ali_conf['bucket'], ali_conf['pipeline_path'], args.version)
    else:
        pipe.destroy()
    installed_pipe.pop(args.name)
    dumpYaml(pipe_yaml, installed_pipe)

def show_pipe(args):
    check_pipe_exists(args.name)
    pipe_path = installed_pipe[args.name]['path']
    version = installed_pipe[args.name]['tag']
    print dyeOKGREEN('Pipeline {name}({version}) was installed at {path}'.format(name=args.name, version=version, path=pipe_path))
    readme = os.path.join(pipe_path, 'README.md')
    with open(readme) as f:
        print f.read()

def check_pipe_exists(name):
    if name not in installed_pipe:
        print dyeFAIL('No Such Pipeline')
        os._exit(1)

def build_pipe(args):
    if args.name:
        if args.pipe_path:
            print dyeWARNING('pipe_path will be ignore since name is provided')
        check_pipe_exists(args.name)
        pipe_path = installed_pipe[args.name]['path']
        pipe = Pipe(pipe_path)
    elif args.pipe_path and os.path.isdir(args.pipe_path):
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
                   priority=args.priority,
                   overwrite=args.overwrite,
                   verbose = args.verbose)
    else:
        print >> sys.stderr, "parameters.conf is missing."
        os._exit(0)

def config_bcs(args):
    def update_config(conf_file, new_conf):
        if not new_conf:
            return
        conf = {}
        if os.path.exists(conf_file):
            conf = loadYaml(conf_file)
        conf.update(new_conf)
        dumpYaml(conf_file, conf)
        os.chmod(conf_file, 0600)

    ali_conf_file = os.path.expanduser("~/.snap/ali.conf")
    ali_new_conf = {k:v for k,v in args._get_kwargs() if v and k not in ('func', 'access_token', 'mobile')}
    update_config(ali_conf_file, ali_new_conf)

    dingtalk_conf_file = os.path.expanduser("~/.snap/dingtalk.conf")
    dingtalk_new_conf = {k:v for k,v in args._get_kwargs() if v and k in ('access_token', 'mobile')}
    update_config(dingtalk_conf_file, dingtalk_new_conf)

def sync_bcs(args):
    if not args.project:
        projects = [load_project(name) for name, dbfile in db.items()]
        map(lambda x: x.sync(), projects)
    else:
        project = load_project(args.project)
        project.sync()

def stat_bcs(args):
    if not args.project:
        projects = [load_project(name) for name, dbfile in db.items()]
    else:
        projects = [load_project(args.project)]

    print format_project_tbl(projects, args.size, args.cost)

def show_bcs(args):
    try:
        proj = load_project(args.project)
    except KeyError:
        print dyeFAIL("No such project: %s in ~/.snap/db.yaml" % args.project)
        os._exit(1)

    print format_detail_porject(proj)

def update_bcs(args):
    def update_cron():
        dummy_args = argparse.Namespace(project=args.project, add=False, delete=True, interval=15)
        cron_bcs(dummy_args)
        dummy_args = argparse.Namespace(project=setting['name'], add=True, delete=False, interval=15)
        cron_bcs(dummy_args)

    fields = ('name', 'description', 'owner', 'status', 'max_job', 'run_cnt', 'discount', 'email', 'mns', 'cluster', 'auto_scale')
    setting = {k:v for k,v in args._get_kwargs() if k in fields and v is not None}
    try:
        proj = load_project(args.project)
    except KeyError:
        print dyeFAIL("No such project: %s in ~/.snap/db.yaml" % args.project)
        os._exit(1)

    proj.update(**setting)
    if 'name' in setting:
        db[setting['name']] = db.pop(args.project)
        dumpYaml(db_yaml, db)
        update_cron()

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
    proj = load_project(args.project)
    proj.clean_files(immediate = not args.all_files)
    proj.clean_bcs()

def cost_bcs(args):
    proj = load_project(args.project)
    if args.bill:
        proj.billing(args.bill)

    costs = proj.cost_stat(args.mode)
    print format_cost_tbl(costs).get_string(sortby="total", reversesort=True)

def instance_bcs(args):
    fake_db = DB(':memory:', pipe_path = '', apps = {}, dependencies = {},
        parameters = {'CommonParameters':{'ContractID':'fake', 'project_description': ''}})
    fake_db.mkProj()
    fake_db.mkInstance()
    fake_db.proj.session = fake_db.session
    instances = fake_db.proj.query_instance(args)
    print format_instance_tbl(instances, args.latest).get_string(sortby="price")

def price_bcs(args):
    price_app.run_server(host='0.0.0.0', port=args.port)

def gantt_bcs(args):
    gantt_app.run_server(host='0.0.0.0', port=args.port)

def inspect_bcs(args):
    proj = load_project(args.project)
    proj.interactive_task(args.docker_image, inputs=args.inputs, outputs=args.outputs, instance_type=args.instance, cluster=args.cluster, tid=args.tid, mid=args.mid, timeout=args.timeout)

def resume_bcs(args):
    proj = load_project(args.project)
    proj.resume_progress()

def load_project(name):
    def fuzzy_match(name):
        matches = filter(lambda x:name in x, db.keys())
        n_matches = len(matches)
        if n_matches == 0:
            print dyeFAIL('Project %s not found' % name)
            os._exit(1)
        elif n_matches == 1:
            name = matches.pop()
        else:
            print dyeFAIL('More than one project matches: %s' % matches)
            os._exit(1)
        return name

    if name not in db:
        name = fuzzy_match(name)
    session = new_session(name, db[name])
    proj = session.query(models.Project).filter_by(name = name).one()
    proj.session = session
    proj.logger = new_logger(name, new_log_file_handler(db[name]))
    return proj

def new_session(name, dbfile):
    engine = create_engine('sqlite:///' + dbfile)
    Session = sessionmaker(bind=engine)
    return Session()

def list_task(args):
    proj = load_project(args.project)
    if args.source or args.destination or args.is_write is not None or args.is_immediate is not None:
        tasks = proj.query_mapping_tasks(args)
    else:
        tasks = proj.query_tasks(args)
    print format_tasks_tbl(tasks, args.cost).get_string(sortby="create", reversesort=True)

def show_task(args):
    def show_each_task(task):
        task.show_detail_tbl()
        if args.jobs:
            task.show_bcs_tbl(args.instance)
        if args.mappings:
            task.show_mapping_tbl(args.size)
        if args.depends:
            task.show_depends_tbl()
        if args.script:
            task.show_shell()

    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    map(show_each_task, tasks)

def debug_task(args):
    proj = load_project(args.project)
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
    def question_update():
        if args.id or len(tasks) <= 1 or args.yes:
            return
        ids = ", ".join([str(t.id) for t in tasks])
        if not question(dyeWARNING('tasks({ids}) will be updated, proceed?[y/n]: '.format(ids=ids))):
            os._exit(0)

    setting = {k:v for k,v in args._get_kwargs() if k in ('instance', 'cpu', 'mem', 'docker_image', 'disk_type', 'disk_size', 'debug_mode', 'benchmark', 'mappings') and v is not None}
    if args.state:
        setting['aasm_state'] = args.state

    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    question_update()
    if setting and tasks:
        map(lambda x: x.update(**setting), tasks)
        tasks[0].project.session.commit()
        print "Changes commited."

def do_task(args, status, event):
    if not args.status:
        args.status = status
    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    ids = " ".join(map(lambda x: str(x.id), tasks))
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
redo_task = functools.partial(do_task, status = ['finished', 'cleaned'], event = 'redo')
stop_task = functools.partial(do_task, status = ['pending', 'waiting', 'running'], event = 'stop')
clean_task = functools.partial(do_task, status = ['stopped', 'finished', 'failed'], event = 'clean')

def submit_task(args):
    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    setting = {'aasm_state': 'pending'}
    map(lambda x: x.update(**setting), tasks)
    do_task(args, ['pending'], 'submit')

def kill_task(args):
    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    stop_task(args)
    map(lambda x: x.kill(), tasks)

def sync_task(args):
    def sync_each_task(task):
        task.bcs[-1].poll()
        task.check()

    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    map(sync_each_task, tasks)

def cyto_task(args):
    proj = load_project(args.project)
    proj.cytoscape(args)

def attach_task(args):
    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    tasks[0].attach()

def profile_task(args):
    proj = load_project(args.project)
    tasks = proj.query_tasks(args)
    profiles = proj.profile(tasks)
    profile_file = os.path.expanduser("~/.snap/profile.pkl")
    profiles.to_pickle(profile_file)
    from core.profile import app as profile_app
    profile_app.run_server(host='0.0.0.0', port=args.port)

def add_mapping(args):
    proj = load_project(args.project)
    proj.add_mapping(args)

def list_mapping(args):
    mappings = query_mappings(args)
    print format_mapping_tbl(mappings, args.size).get_string(sortby=args.sort)

def query_mappings(args):
    proj = load_project(args.project)
    if not args.id and (args.task or args.app or args.module or args.status or args.shell != '.'):
        mappings = proj.query_task_mappings(args)
    else:
        mappings = proj.query_mappings(args, fuzzy=args.fuzzy)

    if args.skip_existed:
        mappings = filter(lambda x:not is_mapping_existed(x), mappings)
    return mappings

def is_mapping_existed(mapping):
    if mapping.is_write:
        return os.path.exists(mapping.source)
    else:
        return mapping.exists()

def show_mapping(args):
    def show_each_mapping(mapping):
        mapping.show_detail_tbl(args.size)
        if args.tasks:
            mapping.show_task_tbl()

    proj = load_project(args.project)
    mappings = proj.query_mappings(args, fuzzy=args.fuzzy)
    map(show_each_mapping, mappings)

def update_mapping(args):
    proj = load_project(args.project)
    mappings = proj.session.query(models.Mapping).filter(models.Mapping.id.in_(args.id)).all()
    if args.task:
        args.task = proj.session.query(models.Task).filter(models.Task.id.in_(args.task)).all()

    setting = {k:v for k,v in args._get_kwargs() if k in ('name', 'source', 'destination', 'is_write', 'is_immediate', 'is_required', 'task') and v is not None}

    if setting and mappings:
        try:
            map(lambda x: x.update(**setting), mappings)
            proj.session.commit()
            print "Changes commited."
        except IntegrityError, e:
            proj.session.rollback()
            print dyeFAIL("There might be an identical mapping exists. Failed to change.\n" + str(e))

def remove_mapping(args):
    proj = load_project(args.project)
    proj.remove_mapping(args)

def sync_mapping(args):
    def check_ossutil_exist():
        EXIT_CODE, OSSUTIL_PATH = commands.getstatusoutput('which ossutil')
        if EXIT_CODE != 0:
            print dyeWARNING('ossutil not found.')
            os._exit(1)

    def estimate_size():
        if args.estimate_size:
            download_size = [m.size() for m in mappings if m.is_write and m.exists() and (args.overwrite or not os.path.exists(m.source))]
            upload_size = [m.source_size() for m in mappings if not m.is_write and os.path.exists(m.source) and (args.overwrite or not m.exists())]
            count_and_print_size(download_size, 'download')
            count_and_print_size(upload_size, 'upload')

    def count_and_print_size(sizes, sync_type):
        if sizes:
            count = len(sizes)
            sizes = human_size(sum(sizes))
            print dyeOKBLUE("{count} files({size}) to {sync_type}.".format(count=count, size=sizes, sync_type=sync_type))

    def question_overwrite():
        if args.overwrite:
            print dyeWARNING('The following mappings will be overwrite:')
            overwrite_mappings = [m for m in mappings if (m.is_write and os.path.exists(m.source)) or (not m.is_write and m.exists()) ]
            print format_mapping_tbl(overwrite_mappings)
            if args.yes or question(dyeWARNING('Proceed risky overwrite sync?[y/n]:')):
                return
            else:
                os._exit(0)

    check_ossutil_exist()
    mappings = query_mappings(args)
    estimate_size()
    question_overwrite()

    map(lambda x: x.sync(overwrite=args.overwrite), mappings)

def create_cluster(args):
    kwargs = {k:v for k,v in args._get_kwargs() if k != 'func' and v is not None}
    if args.instance and args.counts and len(args.instance) != len(args.counts):
        print dyeFAIL('The number of instance and counts is not equal.')
        os._exit(1)
    elif not args.instance and args.counts:
        print dyeFAIL('-counts must work with -instance')
        os._exit(1)
    if args.instance and args.price_limit and len(args.instance) != len(args.price_limit):
        print dyeFAIL('The number of instance and price_limit is not equal.')
        os._exit(1)
    elif not args.instance and args.price_limit:
        print dyeFAIL('-price_limit must work with -instance')
        os._exit(1)

    proj = load_project(args.project)
    proj.create_cluster(**kwargs)

def bind_cluster(args):
    proj = load_project(args.project)
    proj.bind_cluster(args.id)

def list_cluster(args):
    from core.ali.bcs import CLIENT
    if args.project:
        proj = load_project(args.project)
        cluster_in_db = [c.id for c in proj.session.query(models.Cluster).all()]
    else:
        cluster_in_db = None
    clusters = CLIENT.list_clusters("", 100)
    print format_cluster_tbl(clusters, cluster_in_db)

def scale_cluster(args):
    instance = map(lambda x: x.replace('.', '-'), args.instance)
    groups = dict(zip(instance, args.counts))
    proj = load_project(args.project)
    if proj.cluster:
        proj.cluster.scale(**groups)

def bid_cluster(args):
    if args.instance and args.price_limit and len(args.instance) != len(args.price_limit):
        print dyeFAIL('The number of instance and counts is not equal.')
        os._exit(1)

    instance = map(lambda x: x.replace('.', '-'), args.instance)
    if not args.price_limit:
        print dyeWARNING('Using SpotAsPriceGo strategy when price limit is 0.')
        price_limit = [0] * len(instance)
        strategy = ['SpotAsPriceGo'] * len(instance)
    else:
        price_limit = args.price_limit
        strategy = [args.strategy] * len(instance)
    groups = [{'group_name':i, 'strategy':s, 'price_limit':p} for i, s, p in  zip(instance, strategy, price_limit)]
    proj = load_project(args.project)
    if proj.cluster:
        map(lambda x:proj.cluster.bid(**x), groups)

def delete_cluster(args):
    from core.ali.bcs import CLIENT
    msg = dyeWARNING('Delete cluster({clusters})?[y/n]:')
    if args.id and (args.yes or question(msg.format(clusters=" ".join(args.id)))):
        map(CLIENT.delete_cluster, args.id)

    if not args.id and args.project:
        proj = load_project(args.project)
        CLIENT.delete_cluster(proj.cluster.id)
        proj.cluster.finish_date = datetime.datetime.now()
        proj.cluster = None
        proj.save()

def log_cluster(args):
    from core.ali.bcs import CLIENT

    if not args.id and args.project:
        proj = load_project(args.project)
        cluster = CLIENT.get_cluster(proj.cluster.id)
        print "\n".join(cluster.OperationLogs)
    elif args.id:
        cluster = CLIENT.get_cluster(args.id)
        print "\n".join(cluster.OperationLogs)

if __name__ == "__main__":
    parsers = argparse.ArgumentParser(
        description = "SNAP is Not A Pipeline.",
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
    #app run
    subparsers_app_run = subparsers_app.add_parser('run',
        help='Run App directly',
        description="This command will submit script to Aliyun bcs for direct running",
        prog='snap app run',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_app_run.add_argument('-name', help = "app name")
    subparsers_app_run.add_argument('-config', help = "config.yaml file")
    subparsers_app_run.add_argument('-param', help = "render from parameter.yaml file. default will be use if not specified.")
    subparsers_app_run.add_argument('-depend', help = "render defaults from dependencies.yaml file. ")
    subparsers_app_run.add_argument('-debug', action='store_true', help = "show debug render info.")
    subparsers_app_run.add_argument('-output', help = "output render result to file. default write to stdout")
    subparsers_app_run.add_argument('-instance', help="Overwrite app instance")
    subparsers_app_run.add_argument('-cpu', help="Overwrite app cpu", type=int)
    subparsers_app_run.add_argument('-mem', help="Overwrite app mem")
    subparsers_app_run.add_argument('-docker_image', help="Overwrite app docker image")
    subparsers_app_run.add_argument('-disk_type', help="Overwrite app disk type")
    subparsers_app_run.add_argument('-disk_size', help="Overwrite app disk size")
    subparsers_app_run.add_argument('-cluster', help="Run on which cluster")
    subparsers_app_run.add_argument('-discount', type=float, help="How much discount you wang.")
    subparsers_app_run.add_argument('-upload', default=False, action='store_true', help="Auto upload scripts")
    subparsers_app_run.add_argument('-all', default=False, action='store_true', help="Run all scripts")
    subparsers_app_run.add_argument('-show_json', default=False, action='store_true', help="Show json")
    subparsers_app_run.set_defaults(func=run_app)
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

    # pipe add
    subparsers_pipe_add = subparsers_pipe.add_parser('add',
        help='install new pipelines.',
        description="This command will add pipeline to local for easy install and deploy. "
        "Install new pipelines.",
        prog='snap pipe add',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_add.add_argument('-url', required=True, help="the git repos url to the pipeline")
    subparsers_pipe_add.add_argument('-name', help="the pipeline name")
    subparsers_pipe_add.add_argument('-destination', default=os.path.expanduser('~/.snap/pipelines/'), help="where to place pipeline")
    subparsers_pipe_add.add_argument('-overwrite', default=False, action='store_true', help="overwrite existed pipeline.")
    subparsers_pipe_add.set_defaults(func=add_pipe)

    # pipe list
    subparsers_pipe_list = subparsers_pipe.add_parser('list',
        help='list installed pipelines.',
        description="This command will list installed pipeline",
        prog='snap pipe list',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_list.add_argument('-name', help="the pipeline name")
    subparsers_pipe_list.set_defaults(func=list_pipe)

    # pipe switch
    subparsers_pipe_switch = subparsers_pipe.add_parser('switch',
        help='switch pipeline version.',
        description="This command will switch pipeline version",
        prog='snap pipe switch',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_switch.add_argument('-name', required=True, help="the pipeline name")
    subparsers_pipe_switch.add_argument('-version', required=True, help="the pipeline version")
    subparsers_pipe_switch.set_defaults(func=switch_pipe)

    # pipe upgrade
    subparsers_pipe_upgrade = subparsers_pipe.add_parser('upgrade',
        help='pull and switch to latest version.',
        description="This command will pull and switch to latest pipeline version",
        prog='snap pipe upgrade',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_upgrade.add_argument('-name', required=True, help="the pipeline name")
    subparsers_pipe_upgrade.add_argument('-refspec', default='+refs/heads/*:refs/remotes/origin/*', help="refspec of git repo")
    subparsers_pipe_upgrade.set_defaults(func=upgrade_pipe)

    # pipe deploy
    subparsers_pipe_deploy = subparsers_pipe.add_parser('deploy',
        help='deploy pipeline to Aliyun OSS.',
        description="This command will deploy pipeline to Aliyun OSS",
        prog='snap pipe deploy',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_deploy.add_argument('-name', required=True, help="the pipeline name")
    subparsers_pipe_deploy.add_argument('-version', help="the pipeline version")
    subparsers_pipe_deploy.set_defaults(func=deploy_pipe)

    # pipe remove
    subparsers_pipe_remove = subparsers_pipe.add_parser('remove',
        help='remove pipeline on Aliyun OSS and local.',
        description="This command will delete pipeline on Aliyun OSS and local",
        prog='snap pipe remove',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_remove.add_argument('-name', required=True, help="the pipeline name")
    subparsers_pipe_remove.add_argument('-version', default='', help="the pipeline version")
    subparsers_pipe_remove.add_argument('-aliyun', default=False, action='store_true', help="delete files on aliyun OSS")
    subparsers_pipe_remove.set_defaults(func=remove_pipe)

    # pipe show
    subparsers_pipe_show = subparsers_pipe.add_parser('show',
        help='show pipeline detail informatioin',
        description="This command will show pipeline detail information",
        prog='snap pipe show',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_show.add_argument('-name', required=True, help="the pipeline name")
    subparsers_pipe_show.set_defaults(func=show_pipe)

    # pipe build
    subparsers_pipe_build = subparsers_pipe.add_parser('build',
        help='PIPE build shell and dependencies',
        description="This command render all app config.yaml into *.sh with parameters.conf. "
        "Build dependencies and everything needed to run a pipe",
        prog='snap pipe build',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_pipe_build.add_argument('-name', help="the pipeline name")
    subparsers_pipe_build.add_argument('-pipe_path', help="the path to the pipeline")
    subparsers_pipe_build.add_argument('-proj_name', help = "the name of project")
    subparsers_pipe_build.add_argument('-param', help = "render from parameter.yaml file. default will be use if not specified.")
    subparsers_pipe_build.add_argument('-pymonitor_path', help = "path to pymonitor", default='/data/pipeline/RNA_pipeline/RNA_ref/RNA_ref_v1.0/software/monitor')
    subparsers_pipe_build.add_argument('-priority', help = "priority of qsub", default='RD_test')
    subparsers_pipe_build.add_argument('-queue', help = "queue of qsub", default='all.q')
    subparsers_pipe_build.add_argument('-out', help="output everything needed for a project")
    subparsers_pipe_build.add_argument('-overwrite', default=False, action='store_true', help="overwrite snap.db")
    subparsers_pipe_build.add_argument('-verbose', default=False, action='store_true', help="show more info.")
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
    subparsers_bcs_config.add_argument('-pipeline_path', default='pipeline', help="pipeline path on bucket.")
    subparsers_bcs_config.add_argument('-vpc_id', help="VPC id for access other ECS instance.")
    subparsers_bcs_config.add_argument('-vpc_cidr_block', default='172.16.20.0/20', help="VPC cidr block for access other ECS instance.")
    subparsers_bcs_config.add_argument('-tmate_server', help="tmate server IP.")
    subparsers_bcs_config.add_argument('-benchmark_interval', help="tmate server IP.")
    subparsers_bcs_config.add_argument('-access_token', help="Access token for dingtalk notification")
    subparsers_bcs_config.add_argument('-mobile', help="mobile phone for dingtalk notification")
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

    # bcs show
    subparsers_bcs_show = subparsers_bcs.add_parser('show',
        help='Show Project details.',
        description="This command will show projects information",
        prog='snap bcs show',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_show.add_argument('-project', required=True, help="ContractID or ProjectID, default will show all project recorded in ~/.snap/db.yaml")
    subparsers_bcs_show.set_defaults(func=show_bcs)

    # bcs update
    subparsers_bcs_update = subparsers_bcs.add_parser('update',
        help='Update Project info',
        description="This command will modify project infomation",
        prog='snap bcs update',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_update.add_argument('-project', required=True, help="ContractID or ProjectID, default will show all project recorded in ~/.snap/db.yaml")
    subparsers_bcs_update.add_argument('-name', help="new ContractID or ProjectID")
    subparsers_bcs_update.add_argument('-description', help="new description")
    subparsers_bcs_update.add_argument('-owner', help="Who is in charge of this project now")
    subparsers_bcs_update.add_argument('-status', help="Project status")
    subparsers_bcs_update.add_argument('-max_job', type=int, help="Max concurrent running job.")
    subparsers_bcs_update.add_argument('-discount', type=float, help="Expected discount for instances. When discount is 0, SpotStrategy will be SpotAsPriceGo otherwise SpotWithPriceLimit.")
    subparsers_bcs_update.add_argument('-email', help="Which Email address will be sent when job failed.")
    subparsers_bcs_update.add_argument('-mns', help="MNS endpoint for notification.")
    subparsers_bcs_update.add_argument('-cluster', help="Cluster ID for existed cluster.")
    subparsers_bcs_update.add_argument('-auto_scale', default=None, action='store_true', help="add crontab job")
    subparsers_bcs_update.add_argument('-no_auto_scale', default=None, dest='auto_scale', action='store_false', help="This is not a writable mapping")
    subparsers_bcs_update.set_defaults(func=update_bcs)

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

    # bcs resume
    subparsers_bcs_resume = subparsers_bcs.add_parser('resume',
        help='resume previous progress of project after rebuild.',
        description="This command will resume project progress with logs.",
        prog='snap bcs resume',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_resume.add_argument('-project', default=None, required=True, help="ContractID or ProjectID you want to clean")
    subparsers_bcs_resume.set_defaults(func=resume_bcs)

    # bcs cost
    subparsers_bcs_cost = subparsers_bcs.add_parser('cost',
        help='Billing How Much dose porject cost.',
        description="This command will calculate the actual cost of porject by billing files.",
        prog='snap bcs cost',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_cost.add_argument('-project', required=True, help="ContractID or ProjectID")
    subparsers_bcs_cost.add_argument('-bill', help="Aliyun billing files path")
    subparsers_bcs_cost.add_argument('-mode', default='module', choices=('task', 'app', 'module'), help="cost stat level")
    subparsers_bcs_cost.set_defaults(func=cost_bcs)

    # bcs instance
    subparsers_bcs_instance = subparsers_bcs.add_parser('instance',
        help='Show available instances.',
        description="This command will show aviailable instances.",
        prog='snap bcs instance',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_instance.add_argument('-name', help="instance name")
    subparsers_bcs_instance.add_argument('-cpu', type=int, help="how many core")
    subparsers_bcs_instance.add_argument('-mem', type=float, help="memory size")
    subparsers_bcs_instance.add_argument('-disk_type', choices=('SSD', 'HDD'), help="disk type: SSD/HDD")
    subparsers_bcs_instance.add_argument('-disk_size', help="local disk size")
    subparsers_bcs_instance.add_argument('-price', type=float, help="instance price")
    subparsers_bcs_instance.add_argument('-latest', default=False, action='store_true', help="checkout latest spot price.")
    subparsers_bcs_instance.set_defaults(func=instance_bcs)

    #bcs price
    subparsers_bcs_price = subparsers_bcs.add_parser('price',
        help='Show instances history price.',
        description="This command will show instance history price",
        prog='snap bcs price',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_price.add_argument('-port', default=8000, type=int, help="Port expose")
    subparsers_bcs_price.set_defaults(func=price_bcs)
    #bcs task
    #subparsers_bcs_task = subparsers_bcs.add_parser('task',
    #    help='Sync and update task states with Aliyun BCS.',
    #    description="This command will poll and sync task states from Aliyun BCS",
    #    prog='snap bcs task',
    #    formatter_class=argparse.RawTextHelpFormatter)
    #subparsers_bcs_task.add_argument('-project', default=None, help="ContractID or ProjectID, syn all project in ~/.snap/db.yaml")
    #subparsers_bcs_task.set_defaults(func=sync_bcs)

    #bcs inspect
    subparsers_bcs_inspect = subparsers_bcs.add_parser('inspect',
        help='Inspect Project.',
        description="This command will enter interactive shell for inspect.",
        prog='snap bcs inspect',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_inspect.add_argument('-project', default=None, required=True, help="ContractID or ProjectID")
    subparsers_bcs_inspect.add_argument('-docker_image', default='alpine:3.7-2.2.1a-2', help="docker image for the shell")
    subparsers_bcs_inspect.add_argument('-tid', help="task id", nargs="*")
    subparsers_bcs_inspect.add_argument('-inputs', help="input mappings, k:v paris. local_dir:oss_dir", nargs="*")
    subparsers_bcs_inspect.add_argument('-outputs', help="output mappings, k:v paris. local_dir:oss_dir", nargs="*")
    subparsers_bcs_inspect.add_argument('-mid', help="mapping id", nargs="*")
    subparsers_bcs_inspect.add_argument('-instance', default='ecs.sn1.medium', help="which instance to use.")
    subparsers_bcs_inspect.add_argument('-cluster', help="which cluster to use.")
    subparsers_bcs_inspect.add_argument('-timeout', default=600, help="Auto quit timeout.", type=int)
    subparsers_bcs_inspect.set_defaults(func=inspect_bcs)

    # bcs gantt
    subparsers_bcs_gantt = subparsers_bcs.add_parser('gantt',
        help='Show gantt chart.',
        description="This command will show gantt chart",
        prog='snap bcs gantt',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_bcs_gantt.add_argument('-port', default=8000, type=int, help="Port expose")
    subparsers_bcs_gantt.set_defaults(func=gantt_bcs)

    # task
    parsers_task = subparsers.add_parser('task',
        help = "Operations of tasks",
        description = "Start Pause Sync Stats Clean Task",
        prog = 'snap task',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task = parsers_task.add_subparsers()

    # task select common args
    share_task_parser = argparse.ArgumentParser(add_help=False)
    share_task_parser.add_argument('-project', required=True, help="ContractID or ProjectID")
    share_task_parser.add_argument('-id', default=None, help="Task id", nargs="*", type = int)
    share_task_parser.add_argument('-shell', default='.', help="Task shell")
    share_task_parser.add_argument('-status', default=None, help="Task status", nargs="*")
    share_task_parser.add_argument('-app', default=None, help="Task app", nargs="*")
    share_task_parser.add_argument('-module', default=None, help="Task module", nargs="*")
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
    subparsers_task_list.add_argument('-is_write', default=None, dest='is_write', action='store_true', help="This is a writable mapping")
    subparsers_task_list.add_argument('-is_not_write', default=None, dest='is_write', action='store_false', help="This is not a writable mapping")
    subparsers_task_list.add_argument('-is_immediate', default=None, dest='is_immediate', action='store_true', help="This is a immediate mapping")
    subparsers_task_list.add_argument('-is_not_immediate', default=None, dest='is_immediate', action='store_false', help="This is not a immediate mapping")
    subparsers_task_list.add_argument('-is_required', default=None, dest='is_required', action='store_true', help="This is a required mapping")
    subparsers_task_list.add_argument('-is_not_requried', default=None, dest='is_required', action='store_false', help="This is not a required mapping")
    subparsers_task_list.add_argument('-fuzzy', default=False, action='store_true', help="Fuzzy search source and destination")
    subparsers_task_list.add_argument('-cost', default=False, action='store_true', help="Show task cost or not")
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
    subparsers_task_show.add_argument('-size', default=False, action='store_true', help="Show mappings size")
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
    subparsers_task_update.add_argument('-docker_image', help="Update task docker image")
    subparsers_task_update.add_argument('-disk_type', help="Update task disk type")
    subparsers_task_update.add_argument('-disk_size', help="Update task disk size", type=float)
    subparsers_task_update.add_argument('-debug', default=None, dest='debug_mode', action='store_true', help="set task debug to true")
    subparsers_task_update.add_argument('-no-debug', default=None, dest='debug_mode', action='store_false', help="set task debug to false")
    subparsers_task_update.add_argument('-benchmark', default=None, dest='benchmark', action='store_true', help="set task benchmark true")
    subparsers_task_update.add_argument('-no-benchmark', default=None, dest='benchmark', action='store_false', help="set task benchmark false")
    subparsers_task_update.add_argument('-state', help="Update task status")
    subparsers_task_update.add_argument('-mappings', nargs="*", help="Update task mappings. Mapping id is needed.")
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
    subparsers_task_submit = subparsers_task.add_parser('submit',
        help='Submit task immediately and ignore all restraint.',
        description="This command will submit task immediately and ignore all restraint.",
        prog='snap task submit',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_submit.set_defaults(func=submit_task)

    #task kill
    subparsers_task_kill = subparsers_task.add_parser('kill',
        help='Kill running or waiting task immediately.',
        description="This command will submit task immediately and ignore all restraint.",
        prog='snap task kill',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_kill.set_defaults(func=kill_task)

    #task sync
    subparsers_task_sync = subparsers_task.add_parser('sync',
        help='Sync seleted task only.',
        description="This command will sync status of selected tasks.",
        prog='snap task sync',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_sync.set_defaults(func=sync_task)

    #task cyto
    subparsers_task_cyto = subparsers_task.add_parser('cyto',
        help='Show selected task in cytoscape.js',
        description="This command will show selected task dependencies in network.",
        prog='snap task cyto',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_cyto.add_argument('-mode', default='app', choices=('task', 'app', 'module'), help="Update task instance")
    subparsers_task_cyto.add_argument('-layout', default='klay', help="Network layout, such as: cose, cose-bilkent, breadthfirst, dagre, klay, circle, random, grid, concentric etc.")
    subparsers_task_cyto.add_argument('-port', default=8000, type=int, help="Port expose")
    subparsers_task_cyto.add_argument('-compound', default='all', choices=('app', 'module', 'all', 'none'), help="Compound level")
    subparsers_task_cyto.add_argument('-size', default='elapsed', choices=('elapsed', 'cpu', 'mem', 'data'), help="What does size map")
    subparsers_task_cyto.set_defaults(func=cyto_task)

    #task attach
    subparsers_task_attach = subparsers_task.add_parser('attach',
        help='Attach to running task. debug_mode must be True',
        description="This command will attach to running task for easy debug.",
        prog='snap task attach',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_attach.set_defaults(func=attach_task)

    #task profile
    subparsers_task_profile = subparsers_task.add_parser('profile',
        help='profile selected tasks.',
        description="This command will profile tasks to guide optimization.",
        prog='snap task profile',
        parents=[share_task_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_task_profile.add_argument('-port', default=8000, type=int, help="Port expose")
    subparsers_task_profile.set_defaults(func=profile_task)

    # mapping
    parsers_mapping = subparsers.add_parser('mapping',
        help = "Operations of mappings",
        description = "Add Remove List Update Link Unlink Mappings",
        prog = 'snap mapping',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping = parsers_mapping.add_subparsers()

    # mapping select common args
    share_mapping_parser = argparse.ArgumentParser(add_help=False)
    share_mapping_parser.add_argument('-project', required=True, help="ContractID or ProjectID")
    share_mapping_parser.add_argument('-id', default=None, help="Mapping id", nargs="*", type = int)
    share_mapping_parser.add_argument('-name', default=None, help="Mapping name")
    share_mapping_parser.add_argument('-source', default=None, help="Task with source mapping")
    share_mapping_parser.add_argument('-destination', default=None, help="Task with destination mapping")
    share_mapping_parser.add_argument('-is_write', default=None, dest='is_write', action='store_true', help="This is a writable mapping")
    share_mapping_parser.add_argument('-is_not_write', default=None, dest='is_write', action='store_false', help="This is not a writable mapping")
    share_mapping_parser.add_argument('-is_immediate', default=None, dest='is_immediate', action='store_true', help="This is a immediate mapping")
    share_mapping_parser.add_argument('-is_not_immediate', default=None, dest='is_immediate', action='store_false', help="This is not a immediate mapping")
    share_mapping_parser.add_argument('-is_required', default=None, dest='is_required', action='store_true', help="This is a required mapping")
    share_mapping_parser.add_argument('-is_not_required', default=None, dest='is_required', action='store_false', help="This is not a required mapping")
    share_mapping_parser.add_argument('-skip_existed', default=False, dest='skip_existed', action='store_true', help="Skip existed mapping")
    share_mapping_parser.add_argument('-yes', action='store_true', help="Don't ask.")

    #mapping add
    subparsers_mapping_add = subparsers_mapping.add_parser('add',
        help='Add new mapping',
        description="This command will add new mapping to db.",
        prog='snap mapping add',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_add.add_argument('-task', default=None, help="Related Task id", nargs="*", type = int)
    subparsers_mapping_add.set_defaults(func=add_mapping)

    #mapping list
    subparsers_mapping_list = subparsers_mapping.add_parser('list',
        help='list mappings',
        description="This command will list mappings.",
        prog='snap mapping list',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_list.add_argument('-task', default=None, help="Task id", nargs="*", type = int)
    subparsers_mapping_list.add_argument('-shell', default='.', help="Task shell")
    subparsers_mapping_list.add_argument('-status', default=None, help="Task status", nargs="*")
    subparsers_mapping_list.add_argument('-app', default=None, help="Task app")
    subparsers_mapping_list.add_argument('-module', default=None, help="Task module")
    subparsers_mapping_list.add_argument('-size', default=False, action='store_true', help="Show size of mapping")
    subparsers_mapping_list.add_argument('-fuzzy', default=False, action='store_true', help="Fuzzy search source and destination")
    subparsers_mapping_list.add_argument('-sort', default='destination', choices=['id', 'name', 'source', 'destination', 'size'], help="Fuzzy search source and destination")
    subparsers_mapping_list.set_defaults(func=list_mapping)

    #mapping show
    subparsers_mapping_show = subparsers_mapping.add_parser('show',
        help='show mappings',
        description="This command will show mappings details.",
        prog='snap mapping show',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_show.add_argument('-fuzzy', default=False, action='store_true', help="Fuzzy search source and destination")
    subparsers_mapping_show.add_argument('-size', default=False, action='store_true', help="Show size of mapping")
    subparsers_mapping_show.add_argument('-tasks', default=False, action='store_true', help="Show related Tasks")
    subparsers_mapping_show.set_defaults(func=show_mapping)

    #mapping update
    subparsers_mapping_update = subparsers_mapping.add_parser('update',
        help='update mappings',
        description="This command will update mappings.",
        prog='snap mapping update',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_update.add_argument('-task', default=None, help="Related Task id", nargs="*", type = int)
    subparsers_mapping_update.set_defaults(func=update_mapping)

    #mapping rm
    subparsers_mapping_remove = subparsers_mapping.add_parser('remove',
        help='remove mappings',
        description="This command will remove mappings.",
        prog='snap mapping remove',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_remove.add_argument('-task', default=None, help="Related Task id", nargs="*", type = int)
    subparsers_mapping_remove.add_argument('-fuzzy', default=False, action='store_true', help="Fuzzy search source and destination")
    subparsers_mapping_remove.set_defaults(func=remove_mapping)

    #mapping sync
    subparsers_mapping_sync = subparsers_mapping.add_parser('sync',
        help='sync mappings',
        description="This command will download or upload mappings.",
        prog='snap mapping sync',
        parents=[share_mapping_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_mapping_sync.add_argument('-fuzzy', default=False, action='store_true', help="Fuzzy search source and destination")
    subparsers_mapping_sync.add_argument('-task', default=None, help="Related Task id", nargs="*", type = int)
    subparsers_mapping_sync.add_argument('-shell', default='.', help="Task shell")
    subparsers_mapping_sync.add_argument('-status', default=None, help="Task status", nargs="*")
    subparsers_mapping_sync.add_argument('-app', default=None, help="Task app")
    subparsers_mapping_sync.add_argument('-module', default=None, help="Task module")
    subparsers_mapping_sync.add_argument('-overwrite', default=False, action='store_true', help="overwrite snap.db")
    subparsers_mapping_sync.add_argument('-estimate_size', default=False, action='store_true', help="estimate sync data size")
    subparsers_mapping_sync.set_defaults(func=sync_mapping)

    # cluster
    parsers_cluster = subparsers.add_parser('cluster',
        help = "Operations of cluster",
        description = "Create List Show Update Delete Clusters",
        prog = 'snap cluster',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster = parsers_cluster.add_subparsers()

    # mapping select common args
    share_cluster_parser = argparse.ArgumentParser(add_help=False)
    share_cluster_parser.add_argument('-project', required=False, help="ContractID or ProjectID")
    share_cluster_parser.add_argument('-id', required=True, help="Cluster ID")

    #cluster create
    subparsers_cluster_create = subparsers_cluster.add_parser('create',
        help='create clusters for project',
        description="This command will create new cluster on Bcs.",
        prog='snap cluster create',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_create.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_create.add_argument('-image', help="Defualt instance image for cluster.")
    subparsers_cluster_create.add_argument('-instance', help="Which instance to use.", nargs="*")
    subparsers_cluster_create.add_argument('-counts', help="How many instance do you want.", nargs="*", type=int)
    subparsers_cluster_create.add_argument('-price_limit', help="Price limit of instances.", nargs="*", type=float)
    subparsers_cluster_create.add_argument('-disk_type', help="Cluster disk type. [system.cloud_ssd, data.cloud_efficiency, data.ephemeral_ssd]")
    subparsers_cluster_create.add_argument('-disk_size', help="Cluster disk size", type=int)
    subparsers_cluster_create.add_argument('-mount_point', help="Mount point for data disk.")
    subparsers_cluster_create.add_argument('-vpc_id', help="VPC id for access other ECS instance.")
    subparsers_cluster_create.add_argument('-vpc_cidr_block', help="VPC cidr block for access other ECS instance.")
    subparsers_cluster_create.set_defaults(func=create_cluster)

    #cluster bind
    subparsers_cluster_bind = subparsers_cluster.add_parser('bind',
        help='bind existed cluster to project',
        description="This command will bind cluster to project.",
        prog='snap cluster bind',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_bind.add_argument('-project', required=True, help="ContractID or ProjectID")
    subparsers_cluster_bind.add_argument('-id', required=True, help="Cluster ID")
    subparsers_cluster_bind.set_defaults(func=bind_cluster)

    #cluster list
    subparsers_cluster_list = subparsers_cluster.add_parser('list',
        help='list existed clusters',
        description="This command will list clusters.",
        prog='snap cluster list',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_list.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_list.set_defaults(func=list_cluster)

    #cluster scale
    subparsers_cluster_scale = subparsers_cluster.add_parser('scale',
        help='scale cluster instances',
        description="This command will scale instances in clusters.",
        prog='snap cluster scale',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_scale.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_scale.add_argument('-id', required=False, help="Cluster ID")
    subparsers_cluster_scale.add_argument('-instance', help="Which instance to use.", nargs="*")
    subparsers_cluster_scale.add_argument('-counts', help="How many instance do you want.", nargs="*", type=int)
    subparsers_cluster_scale.set_defaults(func=scale_cluster)

    #cluster bid
    subparsers_cluster_bid = subparsers_cluster.add_parser('bid',
        help='bid cluster instances price',
        description="This command will change instances price limit in clusters.",
        prog='snap cluster bid',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_bid.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_bid.add_argument('-id', required=False, help="Cluster ID")
    subparsers_cluster_bid.add_argument('-instance', help="Which instance to change.", nargs="*")
    subparsers_cluster_bid.add_argument('-strategy', choices=['SpotWithPriceLimit', 'SpotAsPriceGo'],
                                        default='SpotWithPriceLimit', required=False, help="Spot strategy")
    subparsers_cluster_bid.add_argument('-price_limit', required=False, help="Price limit of instances.", nargs="*", type=float)
    subparsers_cluster_bid.set_defaults(func=bid_cluster)

    #cluster delete
    subparsers_cluster_delete = subparsers_cluster.add_parser('delete',
        help='delete clusters',
        description="This command will list clusters.",
        prog='snap cluster delete',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_delete.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_delete.add_argument('-id', required=False, help="Cluster ID", nargs="*")
    subparsers_cluster_delete.add_argument('-yes', action='store_true', help="Don't ask.")
    subparsers_cluster_delete.set_defaults(func=delete_cluster)

    #cluster log
    subparsers_cluster_log = subparsers_cluster.add_parser('log',
        help='show clusters log',
        description="This command will list clusters.",
        prog='snap cluster log',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers_cluster_log.add_argument('-project', required=False, help="ContractID or ProjectID")
    subparsers_cluster_log.add_argument('-id', required=False, help="Cluster ID")
    subparsers_cluster_log.set_defaults(func=log_cluster)

if __name__ == '__main__':
    db_yaml = os.path.expanduser("~/.snap/db.yaml")
    db = yaml2dict(db_yaml)
    pipe_yaml = os.path.expanduser("~/.snap/pipe.yaml")
    installed_pipe = yaml2dict(pipe_yaml)
    argslist = sys.argv[1:]
    if len(argslist) > 0:
        args = parsers.parse_args(argslist)
        args.func(args)
    else:
        parsers.print_help()
        os._exit(0)
