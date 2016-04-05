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
        print >> sys.stderr, "app name is not optional."

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

if __name__ == '__main__':
    argslist = sys.argv[1:]
    if len(argslist) > 0:
        args = parsers.parse_args(argslist)
        args.func(args)
    else:
        parsers.print_help()
        os._exit(0)
