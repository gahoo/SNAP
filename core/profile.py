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

app = dash.Dash()

app.scripts.config.serve_locally = True

profile_file = os.path.expanduser("~/.snap/profile.pkl")
profiles = pd.read_pickle(profile_file)
profiles.Time = profiles.Time.astype('int64')
column_options = [{'label': c , 'value':c} for c in  profiles.columns.values.tolist()] + [{'label': 'None', 'value': None}]

def color_palette(elements, ptype='qual', palette='Paired'):
    n_element = len(elements)
    if n_element <= 11:
        cnt = str(n_element)
        colors = cl.scales[cnt][ptype][palette]
    else:
        cnt = '11'
        palettes = cl.scales[cnt][ptype][palette]
        colors = cl.interp(palettes, n_element)
    colors = cl.to_rgb(colors)
    return dict(zip(elements, colors))

program_color = color_palette(profiles.Program.unique())

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
            html.Span('X-axis'),
            dcc.Dropdown(
                options = column_options,
                placeholder = 'Xaxis',
                value = 'Time',
                id = 'xaxis')
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.Span('Y-axis'),
            dcc.Dropdown(
                options = column_options,
                value = '%CPU',
                id = 'yaxis')
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.Span('Size'),
            dcc.Dropdown(
                options = column_options,
                value = None,
                id = 'size_mapper')
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.Span('Type'),
            dcc.Dropdown(
                options = map(lambda x:{'label':x, 'value':x}, ['scatter', 'box', 'heatmap']),
                value = 'scatter',
                id = 'figure_type')
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.Span('Type'),
            dcc.Dropdown(
                options = map(lambda x:{'label':x, 'value':x}, ['markers', 'lines', 'markers+lines']),
                value = 'markers+lines',
                id = 'figure_mode')
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.Span('Z-axis'),
            dcc.Dropdown(
                options = column_options,
                value = None,
                id = 'zaxis')
        ], style={'width': '15%', 'display': 'inline-block'}),
    ]),
    html.Hr(),
    dcc.Graph(
        id='graph-profiles'
    ),
], className="container")

@app.callback(
    Output('datatable-profiles', 'selected_row_indices'),
    [Input('graph-profiles', 'clickData')],
    [State('datatable-profiles', 'selected_row_indices')])
def update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices

@app.callback(
    Output('graph-profiles', 'figure'),
    [Input('datatable-profiles', 'rows'),
     Input('datatable-profiles', 'selected_row_indices'),
     Input('xaxis', 'value'),
     Input('yaxis', 'value'),
     Input('size_mapper', 'value'),
     Input('figure_type', 'value'),
     Input('figure_mode', 'value'),
     Input('zaxis', 'value'),
    ])
def update_figure(rows, selected_row_indices, xaxis_column, yaxis_column, size_column, ftype, fmode, zaxis_column):
    def add_file_trace(fig, filename, row_idx, col_idx=1):
        file_trace = make_file_trace(filename)
        [fig.append_trace(trace, row_idx, col_idx) for trace in file_trace]

    def make_file_trace(filename, merge=False):
        file_df = dff[dff.file == filename]
        if merge:
            pass
        else:
            return [make_program_trace(filename, program) for program in file_df.Program.unique()]

    def make_program_trace(filename, program):
        program_trace = dff[(dff.file == filename) & (dff.Program == program)]
        if size_column:
            scaled_size = 12 * (0.5 + (program_trace[size_column] - dff[size_column].min()) / dff[size_column].max())
        else:
            scaled_size = 6
        data = {
            'type': ftype,
            'name': program}

        if xaxis_column:
            data['x'] = program_trace[xaxis_column]
        if yaxis_column:
            data['y'] = program_trace[yaxis_column]
        if ftype == 'heatmap' and zaxis_column:
            data['z'] = program_trace[zaxis_column]
            data['colorscale'] = 'Viridis'
        if ftype == 'scatter':
            data['mode'] = fmode
        if ftype != 'heatmap':
            data['marker'] = {'color': program_color[program], 'size': scaled_size}

        return data

    dff = pd.DataFrame(rows)
    dff.Time = dff.Time.astype('timedelta64[ns]') + pd.to_datetime('1970/01/01')
    filenames = dff.file.unique()
    fig = plotly.tools.make_subplots(
        rows=len(filenames), cols=1,
        subplot_titles=filenames,
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'

    [add_file_trace(fig, filename, i) for i, filename in enumerate(filenames, start=1)]
    fig['layout']['height'] = 180 * len(filenames)
    max_nchar = max(map(len, dff.Program))
    fig['layout']['margin'] = {
        'l': 7 * max_nchar,
        'r': 10,
        't': 40,
        'b': 80
    }
    #fig['layout']['yaxis3']['type'] = 'log'
    return fig

