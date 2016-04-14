#!/usr/bin/python

import argparse
import yaml
import sys
import os
from core.app import App

def new_app(args):
    if(args.name):
        app = App(args.name)
        app.new()
    else:
        subparsers_app_new.print_help()
        print >> sys.stderr, "app name is not optional."

def init_app(args):
    if(args.name):
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
    app.build(args.param, args.out)

def node_app(args):
    app = init_app(args)
    if args.type == 'all':
        app.buildTestWorkflow(sys.stdout)
    else:
        nodes = app.nodes(args.type)
        if(args.node):
            app.dumpYaml([nodes[args.node]], args.out)
        else:
            app.dumpYaml(nodes.values(), args.out)

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

if __name__ == '__main__':
    argslist = sys.argv[1:]
    if len(argslist) > 0:
        args = parsers.parse_args(argslist)
        args.func(args)
    else:
        parsers.print_help()
        os._exit(0)
