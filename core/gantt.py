import dash
from dash.dependencies import Input, Output, State
from core.misc import *
from core import models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from collections import defaultdict
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import plotly.figure_factory as ff
import plotly.graph_objs as go
import json
import pandas as pd
import numpy as np
import colorlover as cl
import plotly
import os
import pdb
import itertools

db = loadYaml(os.path.expanduser("~/.snap/db.yaml"))
app = dash.Dash()

app.scripts.config.serve_locally = True
ctrl_style = {'width': '25%', 'display': 'inline-block', 'margin': '1'}

def new_session(name, dbfile):
    engine = create_engine('sqlite:///' + dbfile)
    Session = sessionmaker(bind=engine)
    return Session()

def load_project(name):
    session = new_session(name, db[name])
    proj = session.query(models.Project).filter_by(name = name).one()
    proj.session = session
    proj.logger = new_logger(name, new_log_file_handler(db[name]))
    return proj

def make_options(opts):
    return map(lambda x:{'label':x, 'value':x}, opts)

app.layout = html.Div([
    html.H4('Project Gantt'),
    html.Div([
        html.Div([
            html.Label('Show'),
            dcc.Checklist(
                options = make_options(['waited', 'elapsed']),
                values = ['elapsed'],
                id = 'show')
        ], style = {'width': '14%', 'float': 'left', 'margin': '1'}),
        html.Div([
            html.Label('Project'),
            dcc.Dropdown(
                options = make_options(db.keys()),
                id = 'project_id')
        ], style = ctrl_style),
        html.Div([
            html.Label('Mode'),
            dcc.Dropdown(
                options = make_options(['Module', 'App', 'Task', 'Instance']),
                value = 'Module',
                id = 'mode')
        ], style = {'width': '10%', 'display': 'inline-block', 'margin': '1'}),
        html.Div([
            html.Label('Modules'),
            dcc.Dropdown(
                multi=True,
                id = 'modules')
        ], style = ctrl_style),
        html.Div([
            html.Label('Apps'),
            dcc.Dropdown(
                multi=True,
                id = 'apps')
        ], style = ctrl_style),
    ]),
    html.Hr(),
    dcc.Graph(
        id='graph-gantt'
    ),
    dcc.Checklist(
        options = make_options(['boxplot']),
        values = [],
        id = 'boxplot',
        style = {'float': 'right'}),
], className="container")

@app.callback(
    Output('modules', 'options'),
    [Input('project_id', 'value')]
)
def set_modules_options(project_id):
    proj = load_project(project_id)
    modules = proj.session.query(models.Module).all()
    options=make_options([m.name for m in modules])
    return options

@app.callback(
    Output('apps', 'options'),
    [Input('project_id', 'value'),
     Input('modules', 'value'),]
)
def set_apps_options(project_id, modules):
    proj = load_project(project_id)
    apps = proj.session.query(models.App).all()
    if modules:
        apps = [a for a in apps if a.module.name in modules]
    options=make_options([a.name for a in apps])
    return options

@app.callback(
    Output('graph-gantt', 'figure'),
    [
     Input('project_id', 'value'),
     Input('mode', 'value'),
     Input('modules', 'value'),
     Input('apps', 'value'),
     Input('show', 'values'),
     Input('boxplot', 'values'),
    ])
def update_figure(project_id, mode, modules, apps, show, boxplot):
    def pick_job(jobs):
        jobs = filter(lambda x:x.status == 'Finished', jobs)
        return jobs[-1]

    def filter_jobs(jobs):
        if modules:
            jobs = filter(lambda x:x.task.module.name in modules, jobs)
        if apps:
            jobs = filter(lambda x:x.task.app.name in apps, jobs)

        return jobs

    def build_task(job):
        return {'Task': get_task_name[mode](job), 'Start':job.__getattribute__(job_start), 'Finish':job.__getattribute__(job_finish)}

    get_task_name = {
        'Task': lambda x:os.path.basename(x.task.shell),
        'App': lambda x:x.task.app.name,
        'Module': lambda x:x.task.module.name,
        'Instance': lambda x:x.instance.name}

    def choose_start_finish():
        if 'waited' in show and 'elapsed' in show:
            return ('create_date', 'finish_date')
        elif 'waited' in show and 'elapsed' not in show:
            return ('create_date', 'start_date')
        elif 'waited' not in show and 'elapsed' in show:
            return ('start_date', 'finish_date')
        elif 'waited' not in show and 'elapsed' not in show:
            return ('start_date', 'finish_date')

    def pick_start(element):
        return element['Start']

    def prepare_gantt(df):
        return ff.create_gantt(df, group_tasks=True)

    def make_task_trace(name, value):
        return {
            'name': name,
            'type': 'box',
            'x': value
        }

    def add_task_time(job):
        diff_time = job['Finish'] - job['Start']
        task_time[job['Task']].append(diff_time.total_seconds() / 3600.0)

    def prepare_boxplot(df):
        map(add_task_time, df)
        data = [make_task_trace(name, value) for name, value in task_time.iteritems()]
        fig = go.Figure(data=data)
        fig['layout']['showlegend'] = False
        return fig

    proj = load_project(project_id)
    jobs = [pick_job(t.bcs) for t in proj.task if t.bcs]
    jobs = filter_jobs(jobs)
    job_start, job_finish = choose_start_finish()
    df = map(build_task, jobs)
    df = sorted(df, key=pick_start)
    task_time =  defaultdict(list)

    if 'boxplot' in boxplot:
        fig = prepare_boxplot(df)
    else:
        fig = prepare_gantt(df)

    max_nchar = max([len(i['Task']) for i in df])
    fig['layout']['height'] = 18 * len(set([i['Task'] for i in df])) + 200
    fig['layout']['margin'] = {
        'l': 8 * max_nchar,
        'r': 10,
        't': 40,
        'b': 80
    }
    return fig

