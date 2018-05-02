import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import numpy as np
import colorlover as cl
import plotly
import os
import pdb
import itertools

app = dash.Dash()

app.scripts.config.serve_locally = True

profile_file = os.path.expanduser("~/.snap/profile.pkl")
profiles = pd.read_pickle(profile_file)
profiles.Time = profiles.Time.astype('int64')
column_options = [{'label': c , 'value':c} for c in profiles.columns.values.tolist() if c != 'index']

def color_palette(elements, ptype='qual', palette='Paired'):
    n_element = len(elements)
    if n_element < 3:
        colors = cl.scales['3'][ptype][palette][:n_element]
    elif n_element <= 11:
        cnt = str(n_element)
        colors = cl.scales[cnt][ptype][palette]
    else:
        cnt = '11'
        palettes = cl.scales[cnt][ptype][palette]
        colors = cl.interp(palettes, n_element)
    colors = cl.to_rgb(colors)
    return dict(zip(elements, colors))

program_color = color_palette(profiles.Program.unique())
overview_color = color_palette(['%CPU', '%MEM', 'disk'], 'qual', 'Pastel1')
ctrl_style = {'width': '14%', 'display': 'inline-block', 'margin': '1'}

app.layout = html.Div([
    html.H4('Task Profile'),
    dt.DataTable(
        rows=profiles.to_dict('records'),

        # optional - sets the order of columns
        columns=sorted(profiles.columns),

        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-profiles'
    ),
    html.Div(id='selected-indexes'),
    html.Hr(),
    html.Div([
        html.Div([
            html.Label('Show:'),
            dcc.RadioItems(
                options = [{'label': 'Overview', 'value': True}, {'label': 'Detail', 'value': False}],
                value = True,
                id = 'overview')
        ], style={'width': '10%', 'float': 'left', 'margin': '1'}),
        html.Div([
            html.Label('X-axis'),
            dcc.Dropdown(
                options = column_options,
                placeholder = 'Xaxis',
                value = 'Time',
                id = 'xaxis')
        ], style = ctrl_style),
        html.Div([
            html.Label('Y-axis'),
            dcc.Dropdown(
                options = column_options,
                value = '%CPU',
                id = 'yaxis')
        ], style = ctrl_style),
        html.Div([
            html.Label('Size'),
            dcc.Dropdown(
                options = column_options,
                value = None,
                id = 'size_mapper')
        ], style = ctrl_style),
        html.Div([
            html.Label('Type'),
            dcc.Dropdown(
                options = map(lambda x:{'label':x, 'value':x}, ['scatter', 'box']),
                value = 'scatter',
                id = 'figure_type')
        ], style = ctrl_style),
        html.Div([
            html.Label('Mode'),
            dcc.Dropdown(
                options = map(lambda x:{'label':x, 'value':x}, ['markers', 'lines', 'markers+lines']),
                value = 'markers+lines',
                id = 'figure_mode')
        ], style = ctrl_style),
        html.Div([
            html.Label('Height'),
            dcc.Slider(
                min = 120, max=800, step=20, value=180,
                id = 'height')
        ], style = ctrl_style),
    ]),
    html.Hr(),
    dcc.Graph(
        id='graph-profiles'
    ),
], className="container")

@app.callback(
    Output('graph-profiles', 'figure'),
    [Input('datatable-profiles', 'rows'),
     Input('overview', 'value'),
     Input('height', 'value'),
     Input('xaxis', 'value'),
     Input('yaxis', 'value'),
     Input('size_mapper', 'value'),
     Input('figure_type', 'value'),
     Input('figure_mode', 'value'),
    ])
def update_figure(rows, overview, height, xaxis_column, yaxis_column, size_column, ftype, fmode):
    def add_file_trace(fig, filename, row_idx, col_idx=1):
        file_trace = make_file_trace(filename)
        if overview:
            enumerate(file_trace, start=1)
            [fig.append_trace(trace, row_idx, i) for i, trace in enumerate(file_trace, start=1)]
        else:
            [fig.append_trace(trace, row_idx, 1) for trace in file_trace]

    def make_file_trace(filename):
        file_df = dff[dff.file == filename].reset_index()
        if overview:
            return make_overview_trace(file_df)
        else:
            return [make_program_trace(file_df, program) for program in file_df.Program.unique()]

    def make_overview_trace(file_df):
        if file_df.data[0]:
            file_df = file_df.rename(columns = {'data': 'disk'})
        else:
            file_df = file_df.rename(columns = {'sys': 'disk'})
        overview_df = file_df.filter(['Time', '%CPU', '%MEM', 'disk']).groupby('Time')
        overview_df = overview_df.agg({'%CPU': 'sum', '%MEM': 'sum', 'disk': 'max'})
        overview_df.index.name = 'Time'
        overview_df.reset_index(inplace=True)
        get_column_data = lambda column: {
            'name': column, 'type': 'scatter', 'fill': 'tonexty',
            'marker': {'color': overview_color[column]},
            'x': overview_df.Time, 'y': overview_df[column]}

        return map(get_column_data, ('%CPU', '%MEM', 'disk'))

    def make_program_trace(file_df, program):
        program_trace = file_df[file_df.Program == program]
        if size_column:
            scaled_size = 12 * (0.5 + (program_trace[size_column] - dff[size_column].min()) / dff[size_column].max())
        else:
            scaled_size = 6
        data = {
            'marker': {'color': program_color[program], 'size': scaled_size},
            'type': ftype,
            'name': program}

        if xaxis_column:
            data['x'] = program_trace[xaxis_column]
        if yaxis_column:
            data['y'] = program_trace[yaxis_column]
        if ftype == 'scatter':
            data['mode'] = fmode

        return data

    dff = pd.DataFrame(rows)
    dff.Time = dff.Time.astype('timedelta64[ns]') + pd.to_datetime('1970/01/01')
    filenames = dff.file.unique()
    if overview:
        ncol = 3
        titles = map(lambda x: ".".join(x), itertools.product(filenames, ['%CPU', '%MEM', 'disk']))
    else:
        ncol = 1
        titles = filenames
    fig = plotly.tools.make_subplots(
        rows=len(filenames), cols=ncol,
        subplot_titles=titles,
        shared_xaxes=True)

    [add_file_trace(fig, filename, i) for i, filename in enumerate(filenames, start=1)]
    fig['layout']['height'] = height * len(filenames)
    max_nchar = max(map(len, dff.Program))
    fig['layout']['margin'] = {
        'l': 8 * max_nchar,
        'r': 10,
        't': 40,
        'b': 80
    }
    return fig

