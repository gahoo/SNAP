import dash
import pdb
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from core.db import DB
from core.models import Instance
from core.formats import format_instance_tbl, format_history_price_tbl

db = DB(':memory:', None, None, {}, None, False)
db.mkInstance()
db.session.commit()
instances = db.session.query(Instance)

app = dash.Dash()

app.layout = html.Div([
    dcc.Dropdown(
        id='instance-type',
        options = [{'label': i.name, 'value': i.name} for i in instances],
        value = ['ecs.sn2.large'],
        multi = True
    ),
    dcc.Slider(
        id='day',
        min=0,
        max=29,
        step=1,
        marks={i: i for i in range(30)},
        value=0,
    ),
    dcc.Graph(id='price-graph')
])

def prepare_price_data(instance_type, day):
    def extract_data(column, zone):
        return map(lambda x: x[column], [p for p in prices if p['ZoneId'] == zone])

    def extract_zone_spot_price(zone):
        return {
            'x': extract_data('Timestamp', zone),
            'y': extract_data('SpotPrice', zone),
            'name': '{name} SpotPrice {zone}'.format(zone=zone, name=instance_type)}

    instance = db.session.query(Instance).filter_by(name = instance_type).one()
    print format_instance_tbl([instance])
    prices = instance.history_price(day)
    print format_history_price_tbl(prices)
    timestamp = map(lambda x: x['Timestamp'], prices)
    origin_price = map(lambda x: x['OriginPrice'], prices)

    price_data = map(extract_zone_spot_price, set(map(lambda x: x['ZoneId'], prices)))
    price_data.append({'x': timestamp, 'y': origin_price, 'name': '%s OriginPrice' % instance_type})
    return price_data

@app.callback(Output('price-graph', 'figure'), [Input('instance-type', 'value'), Input('day', 'value')])
def update_price_graph(instance_types, day):
    data = []
    map(lambda x:data.extend(prepare_price_data(x, day)), instance_types)
    return {'data': data}
